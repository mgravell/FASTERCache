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

public sealed class FASTERDistributedCache : IDistributedCache, IDisposable
{
    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    private readonly FasterKV<string, Payload> _cache;
    private readonly CacheFunctions _functions;
    public FASTERDistributedCache(IOptions<FASTERCacheOptions> options, IServiceProvider? services)
    {
        var config = options.Value;
        var path = config.Directory;
#if NET8_0_OR_GREATER
        _functions = new CacheFunctions(services?.GetService<TimeProvider>() ?? TimeProvider.System);
#else
        _functions = new CacheFunctions(services?.GetService<ISystemClock>() ?? new SystemClock());
#endif
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
    private sealed class CacheFunctions : SimpleFunctions<string, Payload, int>
    {
#if NET8_0_OR_GREATER
        private readonly TimeProvider _time;
        public CacheFunctions(TimeProvider time) => _time = time;
        public long NowTicks => _time.GetUtcNow().UtcTicks;
#else
        private readonly ISystemClock _time;
        public CacheFunctions(ISystemClock? time) => _time = time ?? SharedClock.Instance;
        public long NowTicks => _time.UtcNow.UtcTicks;
        private static class SharedClock
        {
            public static SystemClock Instance = new();
        }
#endif



        public override bool ConcurrentReader(ref string key, ref Payload input, ref Payload value, ref Payload dst, ref ReadInfo readInfo) // enforce expiry
            => value.ExpiryTicks > NowTicks
            && base.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);

        public override bool SingleReader(ref string key, ref Payload input, ref Payload value, ref Payload dst, ref ReadInfo readInfo) // enforce expiry
            => value.ExpiryTicks > NowTicks
            && base.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

        public override bool ConcurrentWriter(ref string key, ref Payload input, ref Payload src, ref Payload dst, ref Payload output, ref UpsertInfo upsertInfo)
        {
            if (src.ExpiryTicks <= NowTicks) // reject expired
            {
                upsertInfo.Action = UpsertAction.CancelOperation;
                return false;
            }
            return base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);
        }
        public override bool SingleWriter(ref string key, ref Payload input, ref Payload src, ref Payload dst, ref Payload output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (src.ExpiryTicks <= NowTicks) // reject expired
            {
                upsertInfo.Action = UpsertAction.CancelOperation;
                return false;
            }
            return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
        }
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
