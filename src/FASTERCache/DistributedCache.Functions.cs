using FASTER.core;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;

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

        private bool IsExpired(ref SpanByte payload) => GetExpiry(ref payload) <= NowTicks;
        internal static long GetExpiry(ref SpanByte payload) => BinaryPrimitives.ReadInt64LittleEndian(payload.AsReadOnlySpan());

        private bool Read(ref Input input, ref SpanByte value, ref Output dst)
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

        public override bool ConcurrentReader(ref SpanByte key, ref Input input, ref SpanByte value, ref Output dst, ref ReadInfo readInfo) => Read(ref input, ref value, ref dst);

        public override bool SingleReader(ref SpanByte key, ref Input input, ref SpanByte value, ref Output dst, ref ReadInfo readInfo) => Read(ref input, ref value, ref dst);

        public override bool Write(ref Input input, ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
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
