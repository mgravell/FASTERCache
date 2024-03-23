using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;

internal sealed class FASTERDistributedCache : IDistributedCache, IDisposable
{
    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    private readonly FasterKV<string, ReadOnlySequence<byte>> _cache;
    public FASTERDistributedCache(IOptions<FASTERCacheOptions> options)
    {
        var config = options.Value;
        var path = config.Directory;
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        var logSettings = config.LogSettings;
        // create decives if not already specified
        logSettings.LogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.log"));
        logSettings.ObjectLogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.obj.log"));

        // Define serializers; otherwise FASTER will use the slower DataContract
        // Needed only for class keys/values
        var serializerSettings = new SerializerSettings<string, ReadOnlySequence<byte>>
        {
            keySerializer = () => new StringSerializer(),
            valueSerializer = () => new BLOBSerializer(),
        };

        // Create instance of store
        _cache = new FasterKV<string, ReadOnlySequence<byte>>(
            size: 1L << 20,
            logSettings: logSettings,
            checkpointSettings: new CheckpointSettings { CheckpointDir = path },
            serializerSettings: serializerSettings,
            comparer: new StringFasterEqualityComparer()
            );
    }

    internal bool IsDisposed { get; private set; }

    void IDisposable.Dispose()
    {
        IsDisposed = true;
        _cache.Dispose();
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
