using FASTER.core;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;

namespace FASTERCache;

internal readonly struct FASTERCacheOutput
{
    public FASTERCacheOutput(long absoluteExpiration, int slidingExpiration, byte[] payload)
    {
        AbsoluteExpiration = absoluteExpiration;
        SlidingExpiration = slidingExpiration;
        Payload = payload;
    }
    public readonly long AbsoluteExpiration;
    public readonly int SlidingExpiration;
    public readonly byte[]? Payload;
}
internal readonly struct FASTERCacheInput
{
    public FASTERCacheInput(OperationFlags flags, IBufferWriter<byte>? writer = null)
    {
        Flags = flags;
        Writer = writer;
        AbsoluteExpiration = 0;
    }
    private FASTERCacheInput(in FASTERCacheInput source, long absoluteExpiration)
    {
        Flags = source.Flags | OperationFlags.WriteSlide;
        Writer = source.Writer;
        AbsoluteExpiration = absoluteExpiration;
    }

    public readonly IBufferWriter<byte>? Writer;
    public readonly OperationFlags Flags;
    public readonly long AbsoluteExpiration;
    public bool ReadToArray => (Flags & OperationFlags.ReadArray) != 0;
    public bool ReadToWriter
    {
        [MemberNotNullWhen(true, nameof(Writer))]
        get => Writer is not null;
    }
    public bool WriteSlide => (Flags & OperationFlags.WriteSlide) != 0;

    public override string ToString() => Flags.ToString();

    internal FASTERCacheInput Slide(long absoluteExpiration)
        => new FASTERCacheInput(in this, absoluteExpiration);

    [Flags]
    public enum OperationFlags
    {
        None = 0,
        ReadArray = 1 << 0,
        WriteSlide = 1 << 1,
    }
}

internal abstract class FASTERCacheFunctions : FunctionsBase<SpanByte, SpanByte, FASTERCacheInput, FASTERCacheOutput, Empty>
{
    public static FASTERCacheFunctions Create(ISystemClock time) => new SystemClockFunctions(time);
#if NET8_0_OR_GREATER
    public static FASTERCacheFunctions Create(TimeProvider time) => new TimeProviderFunctions(time);
#endif
    public static FASTERCacheFunctions Create()
    {
#if NET8_0_OR_GREATER
        return new TimeProviderFunctions(TimeProvider.System);
#else
        return new SystemClockFunctions(SystemClockFunctions.SharedClock);
#endif
    }

    public abstract long NowTicks { get; }

    private bool IsExpired(ref SpanByte payload) => GetExpiry(ref payload) <= NowTicks;
    internal static long GetExpiry(ref SpanByte payload) => BinaryPrimitives.ReadInt64LittleEndian(payload.AsReadOnlySpan());

    private bool Read(ref FASTERCacheInput input, ref SpanByte value, ref FASTERCacheOutput dst)
    {
        var span = value.AsSpan();
        var absolute = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(0, 8));
        if (absolute <= NowTicks) return false;

        var sliding = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8, 4));

        if (input.ReadToWriter) input.Writer.Write(span.Slice(12));

        byte[]? payload = input.ReadToArray ? span.Slice(12).ToArray() : [];
        dst = new(absolute, sliding, payload);
        return true;
    }

    public override bool ConcurrentReader(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte value, ref FASTERCacheOutput dst, ref ReadInfo readInfo) => Read(ref input, ref value, ref dst);

    public override bool SingleReader(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte value, ref FASTERCacheOutput dst, ref ReadInfo readInfo) => Read(ref input, ref value, ref dst);

    private bool Write(in FASTERCacheInput input, ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
    {
        if (input.WriteSlide)
        {
            // in-place overwrite - need existing data
            if (dst.Length < 12)
            {
                upsertInfo.Action = UpsertAction.CancelOperation;
                return false;
            }
            var span = dst.AsSpan();
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(0, 8), input.AbsoluteExpiration);
            return true;
        }
        if (IsExpired(ref src))
        {
            upsertInfo.Action = UpsertAction.CancelOperation;
            return false;
        }

        return Copy(ref src, ref dst);
    }

    static bool Copy(ref SpanByte src, ref SpanByte dst)
    {
        if (dst.Length < src.Length)
        {
            return false; // request more space
        }
        else if (dst.Length > src.Length)
        {
            dst.ShrinkSerializedLength(src.Length);
        }
        src.CopyTo(ref dst);
        return true;
    }

    public override bool ConcurrentWriter(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte src, ref SpanByte dst, ref FASTERCacheOutput output, ref UpsertInfo upsertInfo) => Write(in input, ref src, ref dst, ref upsertInfo);
    public override bool SingleWriter(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte src, ref SpanByte dst, ref FASTERCacheOutput output, ref UpsertInfo upsertInfo, WriteReason reason) => Write(in input, ref src, ref dst, ref upsertInfo);

    public override bool InitialUpdater(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte value, ref FASTERCacheOutput output, ref RMWInfo rmwInfo)
    {
        return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
    }
    public override bool CopyUpdater(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte oldValue, ref SpanByte newValue, ref FASTERCacheOutput output, ref RMWInfo rmwInfo)
        => Copy(ref oldValue, ref newValue);
    public override bool InPlaceUpdater(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte value, ref FASTERCacheOutput output, ref RMWInfo rmwInfo)
    {
        rmwInfo.Action = RMWAction.CancelOperation;
        return false;
    }
    public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo) => true;
    public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo) => true;

#if NET8_0_OR_GREATER
    private sealed class TimeProviderFunctions : FASTERCacheFunctions
    {
        public TimeProviderFunctions(TimeProvider time) => _time = time;
        private readonly TimeProvider _time;
        public override long NowTicks => _time.GetUtcNow().UtcTicks;
    }
#endif
    private sealed class SystemClockFunctions : FASTERCacheFunctions
    {
#if !NET8_0_OR_GREATER
        private static SystemClock? s_sharedClock;
        internal static SystemClock SharedClock => s_sharedClock ??= new();
#endif
        public SystemClockFunctions(ISystemClock time) => _time = time;
        private readonly ISystemClock _time;
        public override long NowTicks => _time.UtcNow.UtcTicks;
    }
}
