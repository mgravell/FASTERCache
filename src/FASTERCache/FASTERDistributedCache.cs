using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Options;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;

internal sealed class FASTERDistributedCache : IDistributedCache, IDisposable
{
    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    private readonly FasterKV<string, Payload> _cache;
    private readonly CacheFunctions _functions;

    public FASTERDistributedCache(IOptions<FASTERCacheOptions> options, IServiceProvider services)
        : this(options.Value, GetClock(services))
    { }
    static object? GetClock(IServiceProvider services)
    {
#if NET8_0_OR_GREATER
        if (services.GetService<TimeProvider>() is { } time)
        {
            return time;
        }
#endif
        return services.GetService<ISystemClock>();
    }

    internal FASTERDistributedCache(FASTERCacheOptions config, object? clock)
    {
        var path = config.Directory;

        CacheFunctions? functions = null;
#if NET8_0_OR_GREATER
        if (clock is TimeProvider timeProvider)
        {
            functions = CacheFunctions.Create(timeProvider);
        }
#endif
        if (functions is null && clock is ISystemClock systemClock)
        {
            functions = CacheFunctions.Create(systemClock);
        }
        functions ??= CacheFunctions.Create();
        _functions = functions;


        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        var logSettings = config.LogSettings;
        // create decives if not already specified
        logSettings.LogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.log"), capacity: config.LogCapacity, deleteOnClose: config.DeleteOnClose);
        logSettings.ObjectLogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.obj.log"), capacity: config.ObjectLogCapacity, deleteOnClose: config.DeleteOnClose);

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

    private bool Slide(ref Payload payload)
    {
        if (payload.SlidingTicks > 0)
        {
            var after = NowTicks + payload.SlidingTicks;
            if (after > payload.ExpiryTicks)
            {
                payload = new Payload(in payload, after);
                return true;
            }
        }
        return false;
    }
    byte[]? IDistributedCache.Get(string key)
    {
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        var (status, payload) = session.Read(key);
        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            if (Slide(ref payload)) // apply sliding expiration
            {
                session.Upsert(ref key, ref payload);
            }
            return payload.AsBytes();
        }
        return null;
    }

    async Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token)
    {
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        var result = await session.ReadAsync(ref key, token: token);
        var status = result.Status;
        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            var tmp = result.Output;
            if (Slide(ref tmp)) // apply sliding expiration
            {
                await session.UpsertAsync(ref key, ref tmp, token: token);
            }
            return result.Output.AsBytes();
        }
        return null;
    }

    void IDistributedCache.Refresh(string key)
    {
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        var (status, payload) = session.Read(key);
        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            if (Slide(ref payload)) // apply sliding expiration
            {
                session.Upsert(ref key, ref payload);
            }
        }
    }

    async Task IDistributedCache.RefreshAsync(string key, CancellationToken token)
    {
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        var result = await session.ReadAsync(ref key, token: token);
        var status = result.Status;
        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            var tmp = result.Output;
            if (Slide(ref tmp)) // apply sliding expiration
            {
                await session.UpsertAsync(ref key, ref tmp, token: token);
            }
        }
    }

    void IDistributedCache.Remove(string key)
    {
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        session.Delete(ref key);
    }

    async Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        await session.DeleteAsync(ref key, token: token);
    
    }
    
    private long NowTicks => _functions.NowTicks;

    private long GetExpiryTicks(DistributedCacheEntryOptions options, out int sliding)
    {
        sliding = 0;
        if (options is not null)
        {
            if (options.SlidingExpiration is not null)
            {
                sliding = checked((int)options.SlidingExpiration.GetValueOrDefault().Ticks);
            }
            if (options.AbsoluteExpiration is not null)
            {
                return options.AbsoluteExpiration.GetValueOrDefault().DateTime.Ticks;
            }
            if (options.AbsoluteExpirationRelativeToNow is not null)
            {
                return NowTicks + options.AbsoluteExpirationRelativeToNow.GetValueOrDefault().Ticks;
            }
            if (sliding != 0)
            {
                return NowTicks + sliding;
            }
        }
        return NowTicks + OneMinuteTicks;
    }

    private static readonly long OneMinuteTicks = TimeSpan.FromMinutes(1).Ticks;

    void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        Payload payload = new(GetExpiryTicks(options, out var sliding), sliding, value);
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        session.Upsert(ref key, ref payload);
    }

    async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        Payload payload = new(GetExpiryTicks(options, out var sliding), sliding, value);
        using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        await session.UpsertAsync(ref key, ref payload, token: token);
    }
}
