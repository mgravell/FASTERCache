using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using Tsavorite.core;
using SpanByteAllocator = Tsavorite.core.SpanByteAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByte, Tsavorite.core.SpanByte, Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordDisposer>>;
using SpanByteStoreFunctions = Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByte, Tsavorite.core.SpanByte, Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordDisposer>;

namespace TsavoriteCache;

/// <summary>
/// Holds the actual Tsavorite kv store; a single store can be shared between multiple
/// cache implementations, for example "distributed cache" and "output cache", with
/// some ref-counting via AddRef/RemoveRef for disposable.
/// </summary>
internal sealed class CacheService
{
    private readonly TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator> _cache;

    public CacheService(IOptions<TsavoriteCacheOptions> options, ILogger<CacheService> logger)
        : this(options.Value, logger) { }

    internal CacheService(TsavoriteCacheOptions config, object? logger)
    {
        var settings = config.Settings ?? new();

        if (logger is not null && settings.logger is null && settings.loggerFactory is null)
        {
            // setup logger
            settings.logger = logger as ILogger;
            settings.loggerFactory = logger as ILoggerFactory;
        }

        // Create instance of store
        _cache = new(
            settings,
            StoreFunctions<SpanByte, SpanByte>.Create(),
            static (settings, storeFunctions) => new SpanByteAllocator<SpanByteStoreFunctions>(settings, storeFunctions));

    }

    int _refCount = 1;

    public void AddRef()
    {
        lock(this)
        {
            if (_refCount < 0) throw new ObjectDisposedException(nameof(CacheService));
            _refCount = checked(_refCount + 1);
        }
    }
    public void RemoveRef()
    {
        bool kill = false;
        lock (this)
        {
            if (_refCount <= 0) throw new ObjectDisposedException(nameof(CacheService));
            if( (_refCount = checked(_refCount - 1)) == 0)
            {
                kill = true;
                _refCount = -1;
            }
        }
        if (kill) _cache.Dispose();
    }

    public ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions, SpanByteStoreFunctions, SpanByteAllocator> CreateSession<Input, Output, Context, Functions>(Functions functions)
        where Functions : ISessionFunctions<SpanByte, SpanByte, Input, Output, Context>
        => _cache.NewSession<Input, Output, Context, Functions>(functions);

}
