using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using System;
using Tsavorite.core;

namespace FASTERCache;

/// <summary>
/// Allows creation of cache instances without requiring dependency injection services
/// </summary>
public sealed class FASTERCacheBuilder
{
    public FASTERCacheBuilder() { }
    public FASTERCacheBuilder(KVSettings<SpanByte, SpanByte> settings) => _options.Settings = settings;

    private CacheService? _service;
    private readonly FASTERCacheOptions _options = new();
    private object? _clock, _logger;
    internal FASTERCacheOptions Options => _options;
    public FASTERCacheBuilder WithOptions(Action<FASTERCacheOptions> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));
        action(_options);
        return this;
    }
    public FASTERCacheBuilder WithSettings(KVSettings<SpanByte, SpanByte>? settings)
    {
        _options.Settings = settings;
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
