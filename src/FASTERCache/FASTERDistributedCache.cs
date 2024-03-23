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

    private readonly FasterKV<string, Payload> _cache;
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
        var serializerSettings = new SerializerSettings<string, Payload>
        {
            keySerializer = () => new StringSerializer(),
            valueSerializer = () => new PayloadSerializer(),
        };

        // Create instance of store
        _cache = new FasterKV<string, Payload>(
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
        using var session = _cache.For(new CacheFunctions()).NewSession<CacheFunctions>();
        var (status, payload) = session.Read(key);
        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            return payload.AsBytes();
        }
        return null;
    }

    async Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token)
    {
        using var session = _cache.For(new CacheFunctions()).NewSession<CacheFunctions>();
        var result = await session.ReadAsync(ref key, token: token);
        var status = result.Status;
        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            return result.Output.AsBytes();
        }
        return null;
    }

    void IDistributedCache.Refresh(string key) { }

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token)
        => Task.CompletedTask;

    void IDistributedCache.Remove(string key)
    {
        using var session = _cache.For(new CacheFunctions()).NewSession<CacheFunctions>();
        session.Delete(ref key);
    }

    async Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        using var session = _cache.For(new CacheFunctions()).NewSession<CacheFunctions>();
        await session.DeleteAsync(ref key, token: token);
    }
    private sealed class CacheFunctions : SimpleFunctions<string, Payload, int>
    {
    }

    private DateTime Now => DateTime.UtcNow;

    private DateTime GetExpiry(DistributedCacheEntryOptions options)
    {
        if (options is not null)
        {
            if (options.AbsoluteExpiration is not null)
            {
                return options.AbsoluteExpiration.GetValueOrDefault().DateTime;
            }
            if (options.AbsoluteExpirationRelativeToNow is not null)
            {
                return Now.Add(options.AbsoluteExpirationRelativeToNow.GetValueOrDefault());
            }
            if (options.SlidingExpiration is not null)
            {
                return Now.Add(options.SlidingExpiration.GetValueOrDefault());
            }
        }
        return Now.AddMinutes(1);
    }
    void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        Payload payload = new(GetExpiry(options), value);
        using var session = _cache.For(new CacheFunctions()).NewSession<CacheFunctions>();
        session.Upsert(ref key, ref payload);
     }

    async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        Payload payload = new(GetExpiry(options), value);
        using var session = _cache.For(new CacheFunctions()).NewSession<CacheFunctions>();
        await session.UpsertAsync(ref key, ref payload, token: token);
    }
}
