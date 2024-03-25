using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Internal;
using System;
using System.IO;
using static System.Collections.Specialized.BitVector32;

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
    private readonly FASTERCacheOptions _options;
    private object? _clock;
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

#if NET8_0_OR_GREATER
    public FASTERCacheBuilder WithClock(TimeProvider clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }
#endif

    public IDistributedCache Create() => new FASTERDistributedCache(Options, _clock);
}
