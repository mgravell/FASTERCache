using FASTERCache;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Allows FASTER to be used with dependency injection
/// </summary>
public static class FASTERCacheServiceExtensions
{
    public static TValue Get<TState, TValue>(this IDistributedCache cache, string key, in TState state, Deserializer<TState, TValue> deserializer)
    {
        if (cache is IExperimentalBufferCache cacheEx)
        {
            return cacheEx.Get(key, state, deserializer);
        }
        var arr = cache.Get(key);
        return deserializer(state, arr is not null, arr);
    }

    internal static TValue Get<TState, TValue>(this IExperimentalBufferCache cache, string key, in TState state, Deserializer<TState, TValue> deserializer)
    {
        var bw = new ArrayBufferWriter<byte>(); // TODO: recycling
        return deserializer(state, cache.Get(key, bw), bw.WrittenSpan);
    }

    public static ValueTask<TValue> GetAsync<TState, TValue>(this IDistributedCache cache, string key, in TState state, Deserializer<TState, TValue> deserializer, CancellationToken token = default)
    {
        if (cache is IExperimentalBufferCache cacheEx)
        {
            return cacheEx.GetAsync(key, state, deserializer, token);
        }
        var pending = cache.GetAsync(key, token);
        if (!pending.IsCompletedSuccessfully) return Awaited(pending, state, deserializer);

        var arr = pending.GetAwaiter().GetResult();
        return new(deserializer(state, arr is not null, arr));

        static async ValueTask<TValue> Awaited(Task<byte[]?> pending, TState state, Deserializer<TState, TValue> deserializer)
        {
            var arr = await pending;
            return deserializer(state, arr is not null, arr);
        }
    }

    internal static ValueTask<TValue> GetAsync<TState, TValue>(this IExperimentalBufferCache cache, string key, in TState state, Deserializer<TState, TValue> deserializer, CancellationToken token = default)
    {
        var bw = new ArrayBufferWriter<byte>(); // TODO: recycling
        var pending = cache.GetAsync(key, bw, token);
        if (!pending.IsCompletedSuccessfully) return Awaited(pending, bw, state, deserializer);

        return new(deserializer(state, pending.GetAwaiter().GetResult(), bw.WrittenSpan));

        static async ValueTask<TValue> Awaited(ValueTask<bool> pending, ArrayBufferWriter<byte> bw, TState state, Deserializer<TState, TValue> deserializer)
        {
            return deserializer(state, await pending, bw.WrittenSpan);
        }
    }

    public static void AddFASTERCache(this IServiceCollection services, Action<FASTERCacheOptions> setupAction)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(setupAction);
        services.AddLogging();
        if (setupAction is not null)
        {
            services.Configure(setupAction)
                .AddOptionsWithValidateOnStart<FASTERCacheOptions, FASTERCacheOptions.Validator>();
        }
        services.TryAddSingleton<CacheService>();
    }
    public static void AddFASTERDistributedCache(this IServiceCollection services, Action<FASTERCacheOptions>? setupAction = null)
    {
        AddFASTERCache(services, setupAction!); // the shared core handles null (for reuse scenarios)
        services.TryAddSingleton<IDistributedCache, DistributedCache>();
    }
}
