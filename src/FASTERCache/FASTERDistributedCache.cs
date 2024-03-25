using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;

internal sealed class FASTERDistributedCache : IDistributedCache, IDisposable
{
    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    private readonly FasterKV<ReadOnlyMemory<byte>, Memory<byte>> _cache;
    private readonly FASTERCacheFunctions _functions;

    public FASTERDistributedCache(IOptions<FASTERCacheOptions> options, IServiceProvider services)
        : this(options.Value, GetClock(services), services.GetService<ILogger<FASTERDistributedCache>>())
    { }
    static object? GetClock(IServiceProvider services)
    {
#if NET8_0_OR_GREATER
        if (services.GetService<TimeProvider>() is { } time)
        {
            return time;
        }
#endif
        return services.GetService<ISystemClock>();
    }

    internal FASTERDistributedCache(FASTERCacheOptions config, object? clock, object? logger)
    {
        var path = config.Directory;

        FASTERCacheFunctions? functions = null;
#if NET8_0_OR_GREATER
        if (clock is TimeProvider timeProvider)
        {
            functions = FASTERCacheFunctions.Create(timeProvider);
        }
#endif
        if (functions is null && clock is ISystemClock systemClock)
        {
            functions = FASTERCacheFunctions.Create(systemClock);
        }
        functions ??= FASTERCacheFunctions.Create();
        _functions = functions;


        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        var logSettings = config.LogSettings;
        // create decives if not already specified
        logSettings.LogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.log"), capacity: config.LogCapacity, deleteOnClose: config.DeleteOnClose);
        logSettings.ObjectLogDevice ??= Devices.CreateLogDevice(Path.Combine(path, "hlog.obj.log"), capacity: config.ObjectLogCapacity, deleteOnClose: config.DeleteOnClose);

        // Create instance of store
        _cache = new(
            size: 1L << 20,
            logSettings: logSettings,
            checkpointSettings: new CheckpointSettings { CheckpointDir = path },
            loggerFactory: logger as ILoggerFactory,
            logger: logger as ILogger
            );
    }

    internal bool IsDisposed { get; private set; }

    void IDisposable.Dispose()
    {
        IsDisposed = true;
        _cache.Dispose();
    }

    private bool Slide(Span<byte> span)
    {
        var slidingTicks = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8));
        if (slidingTicks > 0)
        {
            var after = NowTicks + slidingTicks;
            long expiryTicks = BinaryPrimitives.ReadInt64LittleEndian(span);
            if (after > expiryTicks)
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, after);
                return true;
            }
        }
        return false;
    }

    private static UTF8Encoding Encoding = new(false);
    const int MAX_STACKALLOC_SIZE = 128;
    static ReadOnlyMemory<byte> LeaseFor(string key, out byte[] lease)
    {
        var length = Encoding.GetByteCount(key);
        lease = ArrayPool<byte>.Shared.Rent(length);
        var actualLength = Encoding.GetBytes(key, 0, key.Length, lease, 0);
        Debug.Assert(length == actualLength);
        return new ReadOnlyMemory<byte>(lease, 0, actualLength);
    }

    static Memory<byte> LeaseFor(byte[] value, out byte[] lease, long absoluteExpiration, int slidingExpiration)
    {
        lease = ArrayPool<byte>.Shared.Rent(value.Length + 12);
        BinaryPrimitives.WriteInt64LittleEndian(new(lease, 0, 8), absoluteExpiration);
        BinaryPrimitives.WriteInt32LittleEndian(new(lease, 8, 4), slidingExpiration);
        value.CopyTo(lease, 12);
        return new Memory<byte>(lease, 0, value.Length + 12);
    }

    private async Task<byte[]?> GetAsync(string key, bool getPayload, CancellationToken token)
    {
        var keyMemory = LeaseFor(key, out var keyLease);

        byte[]? finalResult = null;
        using var session = _cache.For(_functions).NewSession<FASTERCacheFunctions>();
        var result = await session.ReadAsync(keyMemory, token: token);
        var status = result.Status;
        Debug.WriteLine($"Read: {status}");

        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            var payload = result.Output;
            var memory = payload.Item1.Memory.Slice(0, payload.Item2);
            if (Slide(memory.Span)) // apply sliding expiration
            {
                var upsertResult = await session.UpsertAsync(ref keyMemory, ref memory, token: token);
                Debug.WriteLine($"Upsert (slide): {upsertResult.Status}");
            }

            if (getPayload)
            {
                finalResult = memory.Span.Slice(12).ToArray(); // yes, I know
                Debug.WriteLine("Read payload: " + BitConverter.ToString(finalResult));
            }
        }
        ArrayPool<byte>.Shared.Return(keyLease);
        return finalResult;
    }
    private byte[]? Get(string key, bool getPayload)
    {
        var keyMemory = LeaseFor(key, out var keyLease);

        byte[]? finalResult = null;
        using var session = _cache.For(_functions).NewSession<FASTERCacheFunctions>();
        var (status, payload) = session.Read(keyMemory);
        Debug.WriteLine($"Read: {status}");

        if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        {
            var memory = payload.Item1.Memory.Slice(0, payload.Item2);
            if (Slide(memory.Span)) // apply sliding expiration
            {
                var result = session.Upsert(ref keyMemory, ref memory);
                Debug.WriteLine($"Upsert (slide): {result}");
            }

            if (getPayload)
            {
                finalResult = memory.Span.Slice(12).ToArray(); // yes, I know
                Debug.WriteLine("Read payload: " + BitConverter.ToString(finalResult));
            }
        }
        ArrayPool<byte>.Shared.Return(keyLease);
        return finalResult;
    }

    byte[]? IDistributedCache.Get(string key) => Get(key, getPayload: true);

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token) => GetAsync(key, getPayload: true, token: token);

    unsafe void IDistributedCache.Refresh(string key) => Get(key, getPayload: false);

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token) => GetAsync(key, getPayload: false, token: token);

    void IDistributedCache.Remove(string key)
    {
        var keyMemory = LeaseFor(key, out var keyLease);

        using var session = _cache.For(_functions).NewSession<FASTERCacheFunctions>();
        var result = session.Delete(ref keyMemory);
        Debug.WriteLine($"Delete: {result}");
        ArrayPool<byte>.Shared.Return(keyLease);
    }

    async Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        var keyMemory = LeaseFor(key, out var keyLease);

        using var session = _cache.For(_functions).NewSession<FASTERCacheFunctions>();
        var result = await session.DeleteAsync(ref keyMemory, token: token);
        Debug.WriteLine($"Delete: {result.Status}");
        ArrayPool<byte>.Shared.Return(keyLease);
    }

    private long NowTicks => _functions.NowTicks;

    private long GetExpiryTicks(DistributedCacheEntryOptions options, out int sliding)
    {
        sliding = 0;
        if (options is not null)
        {
            if (options.SlidingExpiration is not null)
            {
                sliding = checked((int)options.SlidingExpiration.GetValueOrDefault().Ticks);
            }
            if (options.AbsoluteExpiration is not null)
            {
                return options.AbsoluteExpiration.GetValueOrDefault().DateTime.Ticks;
            }
            if (options.AbsoluteExpirationRelativeToNow is not null)
            {
                return NowTicks + options.AbsoluteExpirationRelativeToNow.GetValueOrDefault().Ticks;
            }
            if (sliding != 0)
            {
                return NowTicks + sliding;
            }
        }
        return NowTicks + OneMinuteTicks;
    }

    private static readonly long OneMinuteTicks = TimeSpan.FromMinutes(1).Ticks;

    void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        var keyMemory = LeaseFor(key, out var keyLease);
        var valueMemory = LeaseFor(value, out var valueLease, GetExpiryTicks(options, out var sliding), sliding); 

        using var session = _cache.For(_functions).NewSession<FASTERCacheFunctions>();

        Debug.WriteLine("Write: " + BitConverter.ToString(valueMemory.Span.Slice(12).ToArray()));
        var result = session.Upsert(ref keyMemory, ref valueMemory);
        Debug.WriteLine($"Upsert: {result}");

        ArrayPool<byte>.Shared.Return(keyLease);
        ArrayPool<byte>.Shared.Return(valueLease);
    }

    async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        var keyMemory = LeaseFor(key, out var keyLease);
        var valueMemory = LeaseFor(value, out var valueLease, GetExpiryTicks(options, out var sliding), sliding);

        using var session = _cache.For(_functions).NewSession<FASTERCacheFunctions>();

        Debug.WriteLine("Write: " + BitConverter.ToString(valueMemory.Span.Slice(12).ToArray()));
        var result = await session.UpsertAsync(ref keyMemory, ref valueMemory, token: token);
        Debug.WriteLine($"Upsert: {result.Status}");

        ArrayPool<byte>.Shared.Return(keyLease);
        ArrayPool<byte>.Shared.Return(valueLease);
    }
}
