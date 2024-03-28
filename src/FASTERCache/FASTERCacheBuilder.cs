using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using System;

namespace FASTERCache;

/// <summary>
/// Allows creation of cache instances without requiring dependency injection services
/// </summary>
public sealed class FASTERCacheBuilder
{
    public FASTERCacheBuilder(string directory)
    {
        if (directory is null) throw new ArgumentNullException(nameof(directory));
        if (string.IsNullOrWhiteSpace(directory)) throw new ArgumentOutOfRangeException(nameof(directory));
        _options = new FASTERCacheOptions { Directory = directory };
    }
    private CacheService? _service;
    private readonly FASTERCacheOptions _options;
    private object? _clock, _logger;
    internal FASTERCacheOptions Options => _options;
    public FASTERCacheBuilder WithOptions(Action<FASTERCacheOptions> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));
        action(Options);
        return this;
    }
    public FASTERCacheBuilder WithClock(ISystemClock clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }
    internal object? Clock => _clock;

    public FASTERCacheBuilder WithLogger(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        return this;
    }
    public FASTERCacheBuilder WithLogger(ILoggerFactory logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        return this;
    }

#if NET8_0_OR_GREATER
    public FASTERCacheBuilder WithClock(TimeProvider clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }
#endif
    internal CacheService GetCacheService() => _service ??= new(Options, _logger);
    public IFASTERDistributedCache CreateDistributedCache() => new DistributedCache(Options, GetCacheService(), _clock);
}
