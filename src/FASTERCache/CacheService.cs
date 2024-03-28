using FASTER.core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.IO;

namespace FASTERCache;

/// <summary>
/// Holds the actual FASTER kv store; a single store can be shared between multiple
/// cache implementations, for example "distributed cache" and "output cache", with
/// some ref-counting via AddRef/RemoveRef for disposabl
/// </summary>
internal sealed class CacheService
{
    private readonly FasterKV<SpanByte, SpanByte> _cache;

    public CacheService(IOptions<FASTERCacheOptions> options, ILogger<CacheService> logger)
        : this(options.Value, logger) { }

    internal CacheService(FASTERCacheOptions config, object? logger)
    {
        var path = config.Directory;
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        var logSettings = config.LogSettings;
        // create decives if not already specified
        logSettings.LogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.log"), capacity: config.LogCapacity, deleteOnClose: config.DeleteOnClose);

        // Create instance of store
        _cache = new(
            size: 1L << 20,
            logSettings: logSettings,
            checkpointSettings: new CheckpointSettings { CheckpointDir = path },
            loggerFactory: logger as ILoggerFactory,
            logger: logger as ILogger
            );
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

    public ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions> CreateSession<Input, Output, Context, Functions>(Functions functions)
        where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
        => _cache.For(functions).NewSession<Functions>();

}
