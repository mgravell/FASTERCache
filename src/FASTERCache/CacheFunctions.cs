using FASTER.core;
using Microsoft.Extensions.Internal;
using System;

namespace FASTERCache;

internal abstract class CacheFunctions : SimpleFunctions<string, Payload, int>
{
    public static CacheFunctions Create(ISystemClock time) => new SystemClockFunctions(time);
#if NET8_0_OR_GREATER
    public static CacheFunctions Create(TimeProvider time) => new TimeProviderFunctions(time);
#endif
    public static CacheFunctions Create()
    {
#if NET8_0_OR_GREATER
        return new TimeProviderFunctions(TimeProvider.System);
#else
        return new SystemClockFunctions(SystemClockFunctions.SharedClock);
#endif
    }

    public abstract long NowTicks { get; }

    public override bool ConcurrentReader(ref string key, ref Payload input, ref Payload value, ref Payload dst, ref ReadInfo readInfo) // enforce expiry
        => value.ExpiryTicks > NowTicks
        && base.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);

    public override bool SingleReader(ref string key, ref Payload input, ref Payload value, ref Payload dst, ref ReadInfo readInfo) // enforce expiry
        => value.ExpiryTicks > NowTicks
        && base.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

    public override bool ConcurrentWriter(ref string key, ref Payload input, ref Payload src, ref Payload dst, ref Payload output, ref UpsertInfo upsertInfo)
    {
        if (src.ExpiryTicks <= NowTicks) // reject expired
        {
            upsertInfo.Action = UpsertAction.CancelOperation;
            return false;
        }
        return base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);
    }
    public override bool SingleWriter(ref string key, ref Payload input, ref Payload src, ref Payload dst, ref Payload output, ref UpsertInfo upsertInfo, WriteReason reason)
    {
        if (src.ExpiryTicks <= NowTicks) // reject expired
        {
            upsertInfo.Action = UpsertAction.CancelOperation;
            return false;
        }
        return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
    }
#if NET8_0_OR_GREATER
    private sealed class TimeProviderFunctions : CacheFunctions
    {
        public TimeProviderFunctions(TimeProvider time) => _time = time;
        private readonly TimeProvider _time;
        public override long NowTicks => _time.GetUtcNow().UtcTicks;
    }
#endif
    private sealed class SystemClockFunctions : CacheFunctions
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
