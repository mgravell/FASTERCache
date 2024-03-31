using System;
using Tsavorite.core;

namespace FASTERCache;

/// <summary>
/// Provides basic SpanByte-compatible function implementation
/// </summary>
internal class CacheFunctionsBase<TInput, TOutput, TContext> : FunctionsBase<SpanByte, SpanByte, TInput, TOutput, TContext>
{
    /// <summary>
    /// Utility function for SpanByte copying, Upsert version.
    /// </summary>
    protected static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
    {
        // First get the full record length and clear it from the extra value space (if there is any). 
        // This ensures all bytes after the used value space are 0, which retains log-scan correctness.

        // For non-in-place operations, the new record may have been revivified, so standard copying procedure must be done;
        // For SpanByte we don't implement DisposeForRevivification, so any previous value is still there, and thus we must
        // zero unused value space to ensure log-scan correctness, just like in in-place updates.

        // IMPORTANT: usedValueLength and fullValueLength use .TotalSize, not .Length, to account for the leading "Length" int.
        upsertInfo.ClearExtraValueLength(ref recordInfo, ref dst, dst.TotalSize);

        // We want to set the used and extra lengths and Filler whether we succeed (to the new length) or fail (to the original length).
        var result = src.TrySafeCopyTo(ref dst, upsertInfo.FullValueLength);
        upsertInfo.SetUsedValueLength(ref recordInfo, ref dst, dst.TotalSize);
        return result;
    }

    /// <summary>
    /// Utility function for SpanByte copying, RMW version.
    /// </summary>
    public static bool DoSafeCopy(ref SpanByte src, ref SpanByte dst, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
    {
        // See comments in upsertInfo overload of this function.
        rmwInfo.ClearExtraValueLength(ref recordInfo, ref dst, dst.TotalSize);
        var result = src.TrySafeCopyTo(ref dst, rmwInfo.FullValueLength);
        rmwInfo.SetUsedValueLength(ref recordInfo, ref dst, dst.TotalSize);
        return result;
    }

    public virtual bool Write(ref TInput input, ref SpanByte src, ref SpanByte dst, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        => DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo);

    public override bool ConcurrentWriter(ref SpanByte key, ref TInput input, ref SpanByte src, ref SpanByte dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        => Write(ref input, ref src, ref dst, ref upsertInfo, ref recordInfo);
    public override bool SingleWriter(ref SpanByte key, ref TInput input, ref SpanByte src, ref SpanByte dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        => Write(ref input, ref src, ref dst, ref upsertInfo, ref recordInfo);

    public override bool InitialUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
    {
        // return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
        throw new NotSupportedException();
    }
    public override bool CopyUpdater(ref SpanByte key, ref TInput input, ref SpanByte oldValue, ref SpanByte newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        => DoSafeCopy(ref oldValue, ref newValue, ref rmwInfo, ref recordInfo);
    public override bool InPlaceUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
    {
        rmwInfo.Action = RMWAction.CancelOperation;
        return false;
    }
    public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;
    public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;
}
