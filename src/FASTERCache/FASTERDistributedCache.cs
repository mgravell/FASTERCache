using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
namespace FASTERCache;

internal sealed class FASTERDistributedCache : IDistributedCache
{
    public FASTERDistributedCache(IOptions<FASTERCacheOptions> options)
    {
        var config = options.Value;
    }

    byte[]? IDistributedCache.Get(string key)
    {
        throw new NotImplementedException();
    }

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    void IDistributedCache.Refresh(string key)
    {
        throw new NotImplementedException();
    }

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    void IDistributedCache.Remove(string key)
    {
        throw new NotImplementedException();
    }

    Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        throw new NotImplementedException();
    }

    Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        throw new NotImplementedException();
    }
}
