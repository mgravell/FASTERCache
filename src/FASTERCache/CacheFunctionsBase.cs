using System;
using Tsavorite.core;

namespace FASTERCache;

/// <summary>
/// Provides basic SpanByte-compatible function implementation
/// </summary>
internal class CacheFunctionsBase<TInput, TOutput, TContext> : SpanByteFunctions<TInput, TOutput, TContext>
{
    public override bool InitialUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        => throw new NotSupportedException();

    public override bool CopyUpdater(ref SpanByte key, ref TInput input, ref SpanByte oldValue, ref SpanByte newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        => throw new NotSupportedException();

    public override bool InPlaceUpdater(ref SpanByte key, ref TInput input, ref SpanByte value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        => throw new NotSupportedException();
}
