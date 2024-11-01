using FASTERCache;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Allows FASTER to be used with dependency injection
/// </summary>
[Experimental("FCACHE002")]
public static class FASTERCacheServiceExtensions
{
    public static TValue? Get<TState, TValue>(this IDistributedCache cache, string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer)
    {
        if (cache is IFASTERDistributedCache fasterCache)
        {
            return fasterCache.Get(key, state, deserializer);
        }
        if (cache is IBufferDistributedCache bufferCache)
        {
            return bufferCache.Get(key, state, deserializer);
        }
        var arr = cache.Get(key);
        return arr is null ? default : deserializer(state, new(arr));
    }

    private static TValue? Get<TState, TValue>(this IBufferDistributedCache cache, string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer)
    {
        var bw = new ArrayBufferWriter<byte>(); // TODO: recycling
        return cache.TryGet(key, bw) ? deserializer(state, new(bw.WrittenMemory)) : default;
    }

    public static ValueTask<TValue?> GetAsync<TState, TValue>(this IDistributedCache cache, string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer, CancellationToken token = default)
    {
        if (cache is IFASTERDistributedCache fasterCache)
        {
            return fasterCache.GetAsync(key, state, deserializer, token);
        }

        if (cache is IBufferDistributedCache bufferCache)
        {
            return bufferCache.GetAsync(key, state, deserializer, token);
        }
        var pending = cache.GetAsync(key, token);
        if (!pending.IsCompletedSuccessfully) return Awaited(pending, state, deserializer);

        var arr = pending.GetAwaiter().GetResult();
        return arr is null ? default : new(deserializer(state, new(arr)));

        static async ValueTask<TValue?> Awaited(Task<byte[]?> pending, TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer)
        {
            var arr = await pending;
            return arr is null ? default : deserializer(state, new(arr));
        }
    }

    private static ValueTask<TValue?> GetAsync<TState, TValue>(this IBufferDistributedCache cache, string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer, CancellationToken token = default)
    {
        var bw = new ArrayBufferWriter<byte>(); // TODO: recycling
        var pending = cache.TryGetAsync(key, bw, token);
        if (!pending.IsCompletedSuccessfully) return Awaited(pending, bw, state, deserializer);

        return pending.GetAwaiter().GetResult() ? new(deserializer(state, new(bw.WrittenMemory))) : default;

        static async ValueTask<TValue?> Awaited(ValueTask<bool> pending, ArrayBufferWriter<byte> bw, TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer)
        {
            return await pending ? deserializer(state, new(bw.WrittenMemory)) : default;
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
