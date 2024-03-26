using FASTER.core;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;

namespace FASTERCache;

internal readonly struct FASTERCacheInput
{
    public FASTERCacheInput(OperationFlags flags, IBufferWriter<byte> writer)
    {
        Flags = flags;
        Writer = writer;
        AbsoluteExpiration = SlidingExpiration = 0;
    }
    public FASTERCacheInput(OperationFlags flags)
    {
        Flags = flags;
        Writer = null;
        AbsoluteExpiration = SlidingExpiration = 0;
    }
    private FASTERCacheInput(in FASTERCacheInput source, long absoluteExpiration, int slidingExpiration)
    {
        Flags = source.Flags;
        Writer = source.Writer;
        AbsoluteExpiration = absoluteExpiration;
        SlidingExpiration = slidingExpiration;
    }
    private FASTERCacheInput(in FASTERCacheInput source, long absoluteExpiration)
    {
        Flags = source.Flags | OperationFlags.WriteSlide;
        Writer = source.Writer;
        AbsoluteExpiration = absoluteExpiration;
        SlidingExpiration = source.SlidingExpiration;
    }

    public FASTERCacheInput WithExpiration(long absoluteExpiration, int slidingExpiration)
        => new(in this, absoluteExpiration, slidingExpiration);

    public readonly IBufferWriter<byte>? Writer;
    public readonly OperationFlags Flags;
    public readonly long AbsoluteExpiration;
    public readonly int SlidingExpiration;
    public bool ReadToArray => (Flags & OperationFlags.ReadArray) != 0;
    public bool ReadToWriter => Writer is not null;
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

internal abstract class FASTERCacheFunctions : FunctionsBase<SpanByte, SpanByte, FASTERCacheInput, byte[]?, Empty>
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

    private bool IsExpired(in SpanByte payload) => GetExpiry(in payload) <= NowTicks;
    internal static long GetExpiry(in SpanByte payload) => BinaryPrimitives.ReadInt64LittleEndian(payload.AsReadOnlySpan());

    private bool Read(ref FASTERCacheInput input, in SpanByte value, ref byte[]? dst)
    {
        var span = value.AsSpan();
        var absolute = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(0, 8));
        if (absolute <= NowTicks) return false;

        var sliding = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8, 4));
        input = input.WithExpiration(absolute, sliding);
        if (input.ReadToArray)
        {
            dst = span.Slice(12).ToArray();
        }
        return true;
    }

    public override bool ConcurrentReader(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte value, ref byte[]? dst, ref ReadInfo readInfo) => Read(ref input, value, ref dst);

    public override bool SingleReader(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte value, ref byte[]? dst, ref ReadInfo readInfo) => Read(ref input, value, ref dst);

    private bool Write(in FASTERCacheInput input, in SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
    {
        if (input.WriteSlide)
        {
            // in-place overwrite - need existing data!
            if (dst.Length == 0)
            {
                upsertInfo.Action = UpsertAction.CancelOperation;
                return false; 
            }
            var span = dst.AsSpan();
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(0, 8), input.AbsoluteExpiration);
            return true;
        }
        if (IsExpired(src))
        {
            upsertInfo.Action = UpsertAction.CancelOperation;
            return false;
        }
        dst = src;
        return true;
    }
    public override bool ConcurrentWriter(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte src, ref SpanByte dst, ref byte[]? output, ref UpsertInfo upsertInfo) => Write(in input, src, ref dst, ref upsertInfo);
    public override bool SingleWriter(ref SpanByte key, ref FASTERCacheInput input, ref SpanByte src, ref SpanByte dst, ref byte[]? output, ref UpsertInfo upsertInfo, WriteReason reason) => Write(in input, src, ref dst, ref upsertInfo);

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
