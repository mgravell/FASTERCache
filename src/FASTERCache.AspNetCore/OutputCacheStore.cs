using FASTER.core;
using Microsoft.AspNetCore.OutputCaching;
using System;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;

internal sealed partial class OutputCacheStore : CacheBase, IOutputCacheStore
{
    internal OutputCacheStore(CacheService cacheService, object? clock) : base(cacheService, clock)
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
    internal sealed class SimpleFunctions : CacheFunctionsBase<Input, Output, Empty>
    {
        private SimpleFunctions() { }
        public static readonly SimpleFunctions Instance = new();
    }
}