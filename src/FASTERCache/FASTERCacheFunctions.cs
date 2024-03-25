using FASTER.core;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;

namespace FASTERCache;

internal abstract class FASTERCacheFunctions : MemoryFunctions<ReadOnlyMemory<byte>, byte, int>
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

    internal static long GetExpiry(in Memory<byte> payload) => BinaryPrimitives.ReadInt64LittleEndian(payload.Span);

    public override bool ConcurrentReader(ref ReadOnlyMemory<byte> key, ref Memory<byte> input, ref Memory<byte> value, ref (IMemoryOwner<byte>, int) dst, ref ReadInfo readInfo)
        => GetExpiry(in value) > NowTicks
        && base.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);

    public override bool SingleReader(ref ReadOnlyMemory<byte> key, ref Memory<byte> input, ref Memory<byte> value, ref (IMemoryOwner<byte>, int) dst, ref ReadInfo readInfo)
        => GetExpiry(in value) > NowTicks
        && base.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

    public override bool ConcurrentWriter(ref ReadOnlyMemory<byte> key, ref Memory<byte> input, ref Memory<byte> src, ref Memory<byte> dst, ref (IMemoryOwner<byte>, int) output, ref UpsertInfo upsertInfo)
    {
        if (GetExpiry(in src) <= NowTicks) // reject expired
        {
            upsertInfo.Action = UpsertAction.CancelOperation;
            return false;
        }
        return base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);
    }
    public override bool SingleWriter(ref ReadOnlyMemory<byte> key, ref Memory<byte> input, ref Memory<byte> src, ref Memory<byte> dst, ref (IMemoryOwner<byte>, int) output, ref UpsertInfo upsertInfo, WriteReason reason)
    {
        if (GetExpiry(in src) <= NowTicks) // reject expired
        {
            upsertInfo.Action = UpsertAction.CancelOperation;
            return false;
        }
        return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
    }

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
