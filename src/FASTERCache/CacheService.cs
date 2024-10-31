using Tsavorite.core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.IO;

using SpanByteStoreFunctions = Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByte, Tsavorite.core.SpanByte, Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordDisposer>;
using SpanByteAllocator = Tsavorite.core.SpanByteAllocator<Tsavorite.core.StoreFunctions<Tsavorite.core.SpanByte, Tsavorite.core.SpanByte, Tsavorite.core.SpanByteComparer, Tsavorite.core.SpanByteRecordDisposer>>;

namespace FASTERCache;

/// <summary>
/// Holds the actual FASTER kv store; a single store can be shared between multiple
/// cache implementations, for example "distributed cache" and "output cache", with
/// some ref-counting via AddRef/RemoveRef for disposabl
/// </summary>
internal sealed class CacheService
{
    private readonly TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator> _cache;

    public CacheService(IOptions<FASTERCacheOptions> options, ILogger<CacheService> logger)
        : this(options.Value, logger) { }

    internal CacheService(FASTERCacheOptions config, object? logger)
    {
        var path = config.Directory;
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        // create devices if not already specified
        config.Settings.LogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.log"), capacity: config.LogCapacity, deleteOnClose: config.DeleteOnClose);
        // setup logger
        config.Settings.logger = logger as ILogger;
        config.Settings.loggerFactory = logger as ILoggerFactory;
        // Create instance of store
        _cache = new(
            config.Settings,
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
