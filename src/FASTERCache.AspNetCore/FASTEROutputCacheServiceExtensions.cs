using FASTERCache;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Allows FASTER to be used with dependency injection
/// </summary>
public static class FASTEROutputCacheServiceExtensions
{
    public static void AddFASTEROutputCacheStore(this IServiceCollection services, Action<FASTERCacheOptions>? setupAction = null)
    {
        services.AddFASTERCache(setupAction!); // the shared core handles null (for reuse scenarios)
        services.TryAddSingleton<IOutputCacheStore, OutputCacheStore>();
    }
}
