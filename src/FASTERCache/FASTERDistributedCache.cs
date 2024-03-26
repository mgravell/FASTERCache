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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static FASTERCache.FASTERCacheInput;

namespace FASTERCache;



internal sealed class FASTERDistributedCache : IDistributedCache, IDisposable, IExperimentalBufferCache
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

    private readonly ConcurrentBag<ClientSession<SpanByte, SpanByte, FASTERCacheInput, FASTERCacheOutput, Empty, FASTERCacheFunctions>> _clientSessions = new();

    private ClientSession<SpanByte, SpanByte, FASTERCacheInput, FASTERCacheOutput, Empty, FASTERCacheFunctions> GetSession()
        => _clientSessions.TryTake(out var session) ? session : _cache.For(_functions).NewSession<FASTERCacheFunctions>();

    private void ReuseSession(ClientSession<SpanByte, SpanByte, FASTERCacheInput, FASTERCacheOutput, Empty, FASTERCacheFunctions> session)
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
    private static void FaultSession(ClientSession<SpanByte, SpanByte, FASTERCacheInput, FASTERCacheOutput, Empty, FASTERCacheFunctions> session)
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

    private bool Slide(in FASTERCacheOutput output, ref FASTERCacheInput input)
    {
        if (output.SlidingExpiration > 0)
        {
            var newAbsolute = NowTicks + output.SlidingExpiration;
            if (newAbsolute > output.AbsoluteExpiration)
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
        if (length > target.Length)
        {
            var arr = ArrayPool<byte>.Shared.Rent(length); ;
            target = new(arr, 0, length);
            return arr;
        }
        target = target.Slice(0, length);
        return [];
    }
    static ReadOnlySpan<byte> WriteKey(Span<byte> target, string key, out byte[] lease)
    {
        var length = Encoding.GetByteCount(key);
        lease = EnsureSize(ref target, length);
        var actualLength = Encoding.GetBytes(key, target);
        Debug.Assert(length == actualLength);
        return target;
    }

    static Memory<byte> WriteKey(string key, out byte[] lease)
    {
        var length = Encoding.GetByteCount(key);
        lease = ArrayPool<byte>.Shared.Rent(length);
        var actualLength = Encoding.GetBytes(key, 0, key.Length, lease, 0);
        Debug.Assert(length == actualLength);
        return new(lease, 0, length);
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

    Memory<byte> WriteValue(ReadOnlySequence<byte> value, out byte[] lease, DistributedCacheEntryOptions? options)
    {
        var absoluteExpiration = GetExpiryTicks(options, out var slidingExpiration);
        var length = checked((int)value.Length + 12);
        lease = ArrayPool<byte>.Shared.Rent(length);
        BinaryPrimitives.WriteInt64LittleEndian(new(lease, 0, 8), absoluteExpiration);
        BinaryPrimitives.WriteInt32LittleEndian(new(lease, 8, 4), slidingExpiration);
        value.CopyTo(new(lease, 12, (int)value.Length));
        return new(lease, 0, length);
    }

    private async Task<TResult?> GetAsync<TResult>(string key, IBufferWriter<byte>? bufferWriter, bool readArray, CancellationToken token, Func<FASTERCacheOutput, TResult> selector)
    {
        var keyMemory = WriteKey(key, out var lease);

        var session = GetSession();
        try
        {
            TResult? finalResult = default;
            using (keyMemory.Pin())
            {
                var fixedKey = SpanByte.FromPinnedMemory(keyMemory);

                var input = new FASTERCacheInput(readArray ? OperationFlags.ReadArray : OperationFlags.None, bufferWriter);

                var readResult = await session.ReadAsync(fixedKey, input, token: token);
                var status = readResult.Status;
                var output = readResult.Output;
                if (status.IsPending)
                {
                    (status, output) = readResult.Complete();
                }

                if (status.IsCompletedSuccessfully && status.Found)
                {
                    finalResult = selector(output);
                    Debug.WriteLine($"Read: {status}");

                    if (Slide(output, ref input))
                    {
                        var upsertResult = await session.BasicContext.UpsertAsync(fixedKey, input, default, token: token);
                        status = upsertResult.Status;
                        if (status.IsPending)
                        {
                            status = upsertResult.Complete();
                        }
                        Debug.WriteLine($"Upsert (slide): {status}");
                    }
                }
                // await session.CompletePendingAsync(true, token: token);
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
                var fixedKey = SpanByte.FromFixedSpan(keySpan);

                var input = new FASTERCacheInput(getPayload ? OperationFlags.ReadArray : OperationFlags.None);
                FASTERCacheOutput output = default;
                var status = session.Read(ref fixedKey, ref input, ref output);
                Debug.WriteLine($"Read: {status}");
                finalResult = output.Payload;

                if (Slide(in output, ref input))
                {
                    SpanByte payload = default;
                    var upsert = session.BasicContext.Upsert(ref fixedKey, ref input, ref payload, ref output);
                    Debug.WriteLine($"Upsert (slide): {upsert}");
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

    private static void ReturnLease(byte[] lease) => ArrayPool<byte>.Shared.Return(lease);

    byte[]? IDistributedCache.Get(string key) => Get(key, getPayload: true);

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token) => GetAsync(key, bufferWriter: null, readArray: true, token: token, static x => x.Payload);

    unsafe void IDistributedCache.Refresh(string key) => Get(key, getPayload: false);

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token) => GetAsync(key, bufferWriter: null, readArray: false, token: token, static _ => true);

    unsafe void IDistributedCache.Remove(string key)
    {
        var keySpan = WriteKey(key.Length <= MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);

        var session = GetSession();
        try
        {
            fixed (byte* ptr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
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
        var keyMemory = WriteKey(key, out var lease);

        var session = GetSession();
        try
        {
            using (keyMemory.Pin())
            {
                var fixedKey = SpanByte.FromPinnedMemory(keyMemory);
                var result = await session.DeleteAsync(ref fixedKey, token: token);
                result.Complete();
                Debug.WriteLine($"Delete: {result.Status}");
                // await session.CompletePendingAsync(true, token: token);
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
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                var fixedValue = SpanByte.FromFixedSpan(valueSpan);
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

    Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, new(value), options, token);

    async Task WriteAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        var keyMemory = WriteKey(key, out var keyLease);
        var valueMemory = WriteValue(value, out var valueLease, options);

        var session = GetSession();
        try
        {
            Debug.WriteLine("Write: " + BitConverter.ToString(valueMemory.Slice(12).ToArray()));
            using (keyMemory.Pin()) // TODO: better pinning
            using (valueMemory.Pin())
            {
                var fixedKey = SpanByte.FromPinnedMemory(keyMemory);
                var fixedValue = SpanByte.FromPinnedMemory(valueMemory);
                var upsertResult = await session.UpsertAsync(ref fixedKey, ref fixedValue, token: token);
                var status = upsertResult.Status;
                if (status.IsPending)
                {
                    status = upsertResult.Complete();
                }
                Debug.WriteLine($"Upsert: {status}");
                //await session.CompletePendingAsync(true, token: token);
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

    Task<bool> IExperimentalBufferCache.GetAsync(string key, IBufferWriter<byte> target, CancellationToken token)
        => GetAsync(key, bufferWriter: target, readArray: false, token: token, selector: static x => true);

    Task IExperimentalBufferCache.SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, value, options, token);
}
