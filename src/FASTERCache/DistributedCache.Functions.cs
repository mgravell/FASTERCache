using FASTER.core;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using static FASTERCache.DistributedCache;

namespace FASTERCache;

partial class DistributedCache
{
    internal abstract class CacheFunctions : CacheFunctionsBase<Input, Output, Empty>
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

        public override bool ConcurrentReader(ref SpanByte key, ref Input input, ref SpanByte value, ref Output dst, ref ReadInfo readInfo)
        {
            var span = value.AsSpan();

            // check for expiration
            var absolute = BinaryPrimitives.ReadInt64LittleEndian(span);
            var now = NowTicks;
            if (absolute <= now)
            {
                readInfo.Action = ReadAction.Expire;
                return false;
            }
            var sliding = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8));

            // copy data out to the query, if needed
            if (input.ReadToWriter) input.Writer.Write(span.Slice(12));
            byte[] payload = input.ReadToArray ? span.Slice(12).ToArray() : [];
            dst = new(absolute, sliding, payload);

            return true;
        }

        public override bool SingleReader(ref SpanByte key, ref Input input, ref SpanByte value, ref Output dst, ref ReadInfo readInfo)
            => ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);

        public override bool Write(ref Input input, ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
            => Copy(ref src, ref dst);

        public override bool InPlaceUpdater(ref SpanByte key, ref Input input, ref SpanByte value, ref Output output, ref RMWInfo rmwInfo)
        {
            var span = value.AsSpan();

            // check for expiration
            var absolute = BinaryPrimitives.ReadInt64LittleEndian(span);
            var now = NowTicks;
            if (absolute <= now)
            {
                rmwInfo.Action = RMWAction.ExpireAndStop;
                return false;
            }
            var sliding = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8));

            // copy data out to the query, if needed
            if (input.ReadToWriter) input.Writer.Write(span.Slice(12));
            byte[] payload = input.ReadToArray ? span.Slice(12).ToArray() : [];
            output = new(absolute, sliding, payload);

            // apply sliding expiration, if needed
            if (sliding > 0)
            {
                var newAbsolute = now + sliding;
                if (newAbsolute > output.AbsoluteExpiration)
                {
                    // update the expiry and commit
                    BinaryPrimitives.WriteInt64LittleEndian(span, newAbsolute);
                }
            }

            // else no change
            return true;
        }

        public override bool NeedInitialUpdate(ref SpanByte key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
            => false;
        public override bool NeedCopyUpdate(ref SpanByte key, ref Input input, ref SpanByte oldValue, ref Output output, ref RMWInfo rmwInfo)
            => true;
        public override bool InitialUpdater(ref SpanByte key, ref Input input, ref SpanByte value, ref Output output, ref RMWInfo rmwInfo)
            => true;
        public override bool CopyUpdater(ref SpanByte key, ref Input input, ref SpanByte oldValue, ref SpanByte newValue, ref Output output, ref RMWInfo rmwInfo)
            => Copy(ref oldValue, ref newValue) && InPlaceUpdater(ref key, ref input, ref newValue, ref output, ref rmwInfo);


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

}
