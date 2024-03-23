using FASTERCache;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

namespace Microsoft.Extensions.DependencyInjection;

public static class FASTERCacheServiceExtensions
{
    public static void AddFASTERCache(this IServiceCollection services, Action<FASTERCacheOptions> setupAction)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(setupAction);
        services.Configure(setupAction)
            .AddOptionsWithValidateOnStart<FASTERCacheOptions, FASTERCacheOptions.Validator>();
        services.TryAddSingleton<IDistributedCache, FASTERDistributedCache>();
    }
}
