using Microsoft.AspNetCore.OutputCaching;

namespace TsavoriteCache;

/// <summary>
/// Allows creation of cache instances without requiring dependency injection services
/// </summary>
public static class TsavoriteCacheBuilderExtensions
{
    public static IOutputCacheStore CreateOutputCacheStore(this TsavoriteCacheBuilder builder) => new OutputCacheStore(builder.GetCacheService(), builder.Clock);
}