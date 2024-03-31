using Tsavorite.core;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime;

namespace FASTERCache;

partial class DistributedCache
{
    internal interface IInputTime
    {
        long NowTicks { get; }
    }
    internal sealed class ByteArrayFunctions : CacheFunctions<BasicInputContext, byte[]>
    {
        private ByteArrayFunctions() { }
        public static readonly ByteArrayFunctions Instance = new();
        protected override void Read(ref BasicInputContext input, ReadOnlySpan<byte> payload, ref byte[] output)
        {
            output = payload.ToArray();
        }
    }
    internal sealed class BooleanFunctions : CacheFunctions<BasicInputContext, bool>
    {
        private BooleanFunctions() { }
        public static readonly BooleanFunctions Instance = new ();

        protected override void Read(ref BasicInputContext input, ReadOnlySpan<byte> payload, ref bool output)
        {
            input.Target?.Write(payload);
            output = true;
        }
    }

    internal abstract class CacheFunctions<TInput, TOutput> : CacheFunctionsBase<TInput, TOutput, Empty>
        where TInput : struct, IInputTime
    {
        public override bool SingleReader(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput dst, ref ReadInfo readInfo)
        {
            var span = value.AsSpan();

            // check for expiration
            var absolute = BinaryPrimitives.ReadInt64LittleEndian(span);
            var now = input.NowTicks;
            if (absolute <= now)
            {
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            // copy data out to the query, if needed
            Read(ref input, span.Slice(12), ref dst);
            return true;
        }

        protected virtual void Read(ref TInput input, ReadOnlySpan<byte> payload, ref TOutput output) { }

        public override bool ConcurrentReader(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        public override bool Write(ref TInput input, ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

        public override bool InPlaceUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var span = value.AsSpan();

            // check for expiration
            var expiration = BinaryPrimitives.ReadInt64LittleEndian(span);
            var now = input.NowTicks;
            if (expiration <= now)
            {
                rmwInfo.Action = RMWAction.ExpireAndStop;
                return false;
            }
            var sliding = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8));

            // copy data out to the query, if needed
            Read(ref input, span.Slice(12), ref output);

            // apply sliding expiration, if needed
            if (sliding > 0)
            {
                var newAbsolute = now + sliding;
                if (newAbsolute > expiration)
                {
                    // update the expiry and commit
                    BinaryPrimitives.WriteInt64LittleEndian(span, newAbsolute);
                }
            }

            // else no change
            return true;
        }

        public override bool NeedInitialUpdate(ref SpanByte key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo)
            => false;
        public override bool NeedCopyUpdate(ref SpanByte key, ref TInput input, ref SpanByte oldValue, ref TOutput output, ref RMWInfo rmwInfo)
            => true;
        public override bool InitialUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => true;
        public override bool CopyUpdater(ref SpanByte key, ref TInput input, ref SpanByte oldValue, ref SpanByte newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            => DoSafeCopy(ref oldValue, ref newValue, ref rmwInfo, ref recordInfo) && InPlaceUpdater(ref key, ref input, ref newValue, ref output, ref rmwInfo, ref recordInfo);
        public override int GetRMWInitialValueLength(ref TInput input)
            => 12;
        public override int GetRMWModifiedValueLength(ref SpanByte value, ref TInput input)
            => value.Length;
    }
}
