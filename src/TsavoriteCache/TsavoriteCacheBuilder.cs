using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using System;
using Tsavorite.core;

namespace TsavoriteCache;

/// <summary>
/// Allows creation of cache instances without requiring dependency injection services
/// </summary>
public sealed class TsavoriteCacheBuilder
{
    public TsavoriteCacheBuilder() { }
    public TsavoriteCacheBuilder(KVSettings<SpanByte, SpanByte> settings) => _options.Settings = settings;

    private CacheService? _service;
    private readonly TsavoriteCacheOptions _options = new();
    private object? _clock, _logger;
    internal TsavoriteCacheOptions Options => _options;
    public TsavoriteCacheBuilder WithOptions(Action<TsavoriteCacheOptions> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));
        action(_options);
        return this;
    }
    public TsavoriteCacheBuilder WithSettings(KVSettings<SpanByte, SpanByte>? settings)
    {
        _options.Settings = settings;
        return this;
    }
    public TsavoriteCacheBuilder WithClock(ISystemClock clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }
    internal object? Clock => _clock;

    public TsavoriteCacheBuilder WithLogger(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        return this;
    }
    public TsavoriteCacheBuilder WithLogger(ILoggerFactory logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        return this;
    }

#if NET8_0_OR_GREATER
    public TsavoriteCacheBuilder WithClock(TimeProvider clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }
#endif
    internal CacheService GetCacheService() => _service ??= new(Options, _logger);
    public ITsavoriteDistributedCache CreateDistributedCache() => new DistributedCache(Options, GetCacheService(), _clock);
}
