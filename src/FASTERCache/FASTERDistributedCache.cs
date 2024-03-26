using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static FASTERCache.FASTERCacheInput;

namespace FASTERCache;



internal sealed class FASTERDistributedCache : IDistributedCache, IDisposable
{
    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    private readonly FasterKV<SpanByte, SpanByte> _cache;
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

    private readonly ConcurrentBag<ClientSession<SpanByte, SpanByte, FASTERCacheInput, byte[]?, Empty, FASTERCacheFunctions>> _clientSessions = new();

    private ClientSession<SpanByte, SpanByte, FASTERCacheInput, byte[]?, Empty, FASTERCacheFunctions> GetSession()
        => _clientSessions.TryTake(out var session) ? session : _cache.For(_functions).NewSession<FASTERCacheFunctions>();

    private void ReuseSession(ClientSession<SpanByte, SpanByte, FASTERCacheInput, byte[]?, Empty, FASTERCacheFunctions> session)
    {
        const int MAX_APPROX_SESSIONS = 20;
        if (_clientSessions.Count <= MAX_APPROX_SESSIONS) // note race, that's fine
        {
            _clientSessions.Add(session);
        }
        else
        {
            session.Dispose();
        }
    }
    private static void FaultSession(ClientSession<SpanByte, SpanByte, FASTERCacheInput, byte[]?, Empty, FASTERCacheFunctions> session)
    {
        if (session is not null)
        {
            // we already know things are unhappy; be cautious
            try { session.Dispose(); }
            catch { }
        }
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

    private bool Slide(ref FASTERCacheInput input)
    {
        if (input.SlidingExpiration > 0)
        {
            var newAbsolute = NowTicks + input.SlidingExpiration;
            if (newAbsolute > input.AbsoluteExpiration)
            {
                input = input.Slide(newAbsolute);
                return true;
            }
        }
        return false;
    }

    private static UTF8Encoding Encoding = new(false);
    const int MAX_STACKALLOC_SIZE = 128;

    private static byte[] EnsureSize(ref Span<byte> target, int length)
    {
        // TODO: stackalloc doesn't seem to be working... what are we committing?
        // if (length > target.Length)
        {
            var arr = ArrayPool<byte>.Shared.Rent(length); ;
            target = new(arr, 0, length);
            return arr;
        }
        //target = target.Slice(0, length);
        //return [];
    }
    static ReadOnlySpan<byte> WriteKey(Span<byte> target, string key, out byte[] lease)
    {
        var length = Encoding.GetByteCount(key);
        lease = EnsureSize(ref target, length);
        var actualLength = Encoding.GetBytes(key, target);
        Debug.Assert(length == actualLength);
        return target;
    }

    ReadOnlySpan<byte> WriteValue(Span<byte> target, byte[] value, out byte[] lease, DistributedCacheEntryOptions? options)
    {
        var absoluteExpiration = GetExpiryTicks(options, out var slidingExpiration);
        lease = EnsureSize(ref target, value.Length + 12);
        BinaryPrimitives.WriteInt64LittleEndian(target.Slice(0, 8), absoluteExpiration);
        BinaryPrimitives.WriteInt32LittleEndian(target.Slice(8, 4), slidingExpiration);
        value.CopyTo(target.Slice(12));
        return target;
    }

    private async Task<byte[]?> GetAsync(string key, bool getPayload, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //var keyMemory = LeaseFor(key, out var keyLease);

        //byte[]? finalResult = null;
        //var session = GetSession();
        //var result = await session.ReadAsync(keyMemory, token: token);
        //var status = result.Status;
        //Debug.WriteLine($"Read: {status}");

        //if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        //{
        //    var payload = result.Output;
        //    var memory = payload.Item1.Memory.Slice(0, payload.Item2);
        //    if (Slide(memory.Span)) // apply sliding expiration
        //    {
        //        var upsertResult = await session.UpsertAsync(ref keyMemory, ref memory, token: token);
        //        Debug.WriteLine($"Upsert (slide): {upsertResult.Status}");
        //    }

        //    if (getPayload)
        //    {
        //        finalResult = memory.Span.Slice(12).ToArray(); // yes, I know
        //        Debug.WriteLine("Read payload: " + BitConverter.ToString(finalResult));
        //    }
        //}
        //ArrayPool<byte>.Shared.Return(keyLease);
        //PutSession(session);
        //return finalResult;
    }
    const int MAX_STACKALLOC = 128;
    private unsafe byte[]? Get(string key, bool getPayload)
    {
        var keySpan = WriteKey(key.Length <= MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);

        byte[]? finalResult = null;
        var session = GetSession();
        try
        {

            fixed (byte* keyPtr = keySpan)
            {
                var fixedKey = SpanByte.FromPointer(keyPtr, keySpan.Length);

                var input = new FASTERCacheInput(getPayload ? OperationFlags.ReadArray : OperationFlags.None);
    
                var status = session.Read(ref fixedKey, ref input, ref finalResult);
                Debug.WriteLine($"Read: {status}");

                if (Slide(ref input))
                {
                    SpanByte payload = default;
                    session.BasicContext.Upsert(ref fixedKey, ref input, ref payload, ref finalResult);
                }
                if (finalResult is not null)
                {
                    Debug.WriteLine("Read payload: " + BitConverter.ToString(finalResult));
                }
                session.CompletePending(true);
            }
            ReturnLease(lease);
            ReuseSession(session);
            return finalResult;
        }
        catch
        {
            FaultSession(session);
            throw;
        }
    }

    private static void ReturnLease(byte[] lease) { }// => ArrayPool<byte>.Shared.Return(lease);

    byte[]? IDistributedCache.Get(string key) => Get(key, getPayload: true);

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token) => GetAsync(key, getPayload: true, token: token);

    unsafe void IDistributedCache.Refresh(string key) => Get(key, getPayload: false);

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token) => GetAsync(key, getPayload: false, token: token);

    unsafe void IDistributedCache.Remove(string key)
    {
        var keySpan = WriteKey(key.Length <= MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);

        var session = GetSession();
        try
        {
            fixed (byte* ptr = keySpan)
            {
                var fixedKey = SpanByte.FromPointer(ptr, keySpan.Length);
                var result = session.Delete(ref fixedKey);
                Debug.WriteLine($"Delete: {result}");
                session.CompletePending(true);
            }
            ReturnLease(lease);
            ReuseSession(session);
        }
        catch
        {
            FaultSession(session);
            throw;
        }
    }

    async Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //var keyMemory = LeaseFor(key, out var keyLease);

        //var session = GetSession();
        //var result = await session.DeleteAsync(ref keyMemory, token: token);
        //Debug.WriteLine($"Delete: {result.Status}");
        //ArrayPool<byte>.Shared.Return(keyLease);
        //PutSession(session);

    }

    private long NowTicks => _functions.NowTicks;

    private long GetExpiryTicks(DistributedCacheEntryOptions? options, out int sliding)
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

    unsafe void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        var keySpan = WriteKey(key.Length <= MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var keyLease);
        var valueSpan = WriteValue(value.Length <= MAX_STACKALLOC - 12 ? stackalloc byte[MAX_STACKALLOC] : default,
            value, out var valueLease, options);

        var session = GetSession();
        try
        {
            Debug.WriteLine("Write: " + BitConverter.ToString(valueSpan.Slice(12).ToArray()));
            fixed (byte* keyPtr = keySpan)
            fixed (byte* valuePtr = valueSpan)
            {
                var fixedKey = SpanByte.FromPointer(keyPtr, keySpan.Length);
                var fixedValue = SpanByte.FromPointer(valuePtr, valueSpan.Length);
                var result = session.Upsert(ref fixedKey, ref fixedValue);
                Debug.WriteLine($"Upsert: {result}");
                session.CompletePending(true);
            }
            ReturnLease(keyLease);
            ReturnLease(valueLease);
            ReuseSession(session);
        }
        catch
        {
            FaultSession(session);
            throw;
        }
    }

    async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //var keyMemory = LeaseFor(key, out var keyLease);
        //var valueMemory = LeaseFor(value, out var valueLease, GetExpiryTicks(options, out var sliding), sliding);

        // var session = GetSession();

        //Debug.WriteLine("Write: " + BitConverter.ToString(valueMemory.Span.Slice(12).ToArray()));
        //var result = await session.UpsertAsync(ref keyMemory, ref valueMemory, token: token);
        //Debug.WriteLine($"Upsert: {result.Status}");

        //ArrayPool<byte>.Shared.Return(keyLease);
        //ArrayPool<byte>.Shared.Return(valueLease);
        //PutSession(session);
    }
}
