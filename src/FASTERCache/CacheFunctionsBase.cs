using FASTER.core;
using System;

namespace FASTERCache;

/// <summary>
/// Provides basic SpanByte-compatible function implementation
/// </summary>
internal class CacheFunctionsBase<TInput, TOutput, TContext> : FunctionsBase<SpanByte, SpanByte, TInput, TOutput, TContext>
{
    protected static bool Copy(ref SpanByte src, ref SpanByte dst)
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

    public virtual bool Write(ref TInput input, ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo)
        => Copy(ref src, ref dst);

    public override bool ConcurrentWriter(ref SpanByte key, ref TInput input, ref SpanByte src, ref SpanByte dst, ref TOutput output, ref UpsertInfo upsertInfo)
        => Write(ref input, ref src, ref dst, ref upsertInfo);
    public override bool SingleWriter(ref SpanByte key, ref TInput input, ref SpanByte src, ref SpanByte dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        => Write(ref input, ref src, ref dst, ref upsertInfo);

    public override bool InitialUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo)
    {
        // return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
        throw new NotSupportedException();
    }
    public override bool CopyUpdater(ref SpanByte key, ref TInput input, ref SpanByte oldValue, ref SpanByte newValue, ref TOutput output, ref RMWInfo rmwInfo)
        => Copy(ref oldValue, ref newValue);
    public override bool InPlaceUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo)
    {
        rmwInfo.Action = RMWAction.CancelOperation;
        return false;
    }
    public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo) => true;
    public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo) => true;
}
