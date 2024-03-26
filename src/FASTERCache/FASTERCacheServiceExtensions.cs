using FASTERCache;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Allows FASTER to be used with dependency injection
/// </summary>
public static class FASTERCacheServiceExtensions
{
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
