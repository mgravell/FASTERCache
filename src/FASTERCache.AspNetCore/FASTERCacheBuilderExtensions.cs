using Microsoft.AspNetCore.OutputCaching;

namespace FASTERCache;

/// <summary>
/// Allows creation of cache instances without requiring dependency injection services
/// </summary>
public static class FASTERCacheBuilderExtensions
{
    public static IOutputCacheStore CreateOutputCacheStore(this FASTERCacheBuilder builder) => new OutputCacheStore(builder.GetCacheService());
}