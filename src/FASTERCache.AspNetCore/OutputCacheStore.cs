using FASTER.core;
using Microsoft.AspNetCore.OutputCaching;
using System;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;

internal sealed partial class OutputCacheStore : CacheBase<OutputCacheStore.Input, OutputCacheStore.Output, Empty, OutputCacheStore.CacheFunctions>, IOutputCacheStore
{
    internal OutputCacheStore(CacheService cacheService) : base(cacheService, new CacheFunctions())
    {
    }

    protected override byte KeyPrefix => (byte)'O';
    public ValueTask EvictByTagAsync(string tag, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public ValueTask<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public ValueTask SetAsync(string key, byte[] value, string[]? tags, TimeSpan validFor, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    internal readonly struct Input { }
    internal readonly struct Output { }
    internal sealed class CacheFunctions : CacheFunctionsBase<Input, Output, Empty> { }
}