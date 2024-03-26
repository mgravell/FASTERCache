using Microsoft.AspNetCore.OutputCaching;
using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;
#if NET8_0_OR_GREATER
internal sealed partial class OutputCacheStore : IOutputCacheBufferStore
{
    ValueTask IOutputCacheBufferStore.SetAsync(string key, ReadOnlySequence<byte> value, ReadOnlyMemory<string> tags, TimeSpan validFor, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    ValueTask<bool> IOutputCacheBufferStore.TryGetAsync(string key, PipeWriter destination, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
#endif