using TsavoriteCache;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace Microsoft.Extensions.DependencyInjection;
#pragma warning restore IDE0130 // Namespace does not match folder structure

/// <summary>
/// Allows Tsavorite to be used with dependency injection
/// </summary>
public static class TsavoriteOutputCacheServiceExtensions
{
    public static void AddTsavoriteOutputCacheStore(this IServiceCollection services, Action<TsavoriteCacheOptions>? setupAction = null)
    {
        services.AddTsavoriteCache(setupAction!); // the shared core handles null (for reuse scenarios)
        services.TryAddSingleton<IOutputCacheStore, OutputCacheStore>();
    }
}
