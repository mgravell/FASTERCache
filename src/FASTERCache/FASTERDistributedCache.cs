using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
namespace FASTERCache;

internal sealed class FASTERDistributedCache : IDistributedCache, IDisposable
{
    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    private readonly FasterKV<SpanByte, SpanByte> _cache;
    private readonly CacheFunctions _functions;

    public FASTERDistributedCache(IOptions<FASTERCacheOptions> options, IServiceProvider services)
        : this(options.Value, GetClock(services))
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

    internal FASTERDistributedCache(FASTERCacheOptions config, object? clock)
    {
        var path = config.Directory;

        CacheFunctions? functions = null;
#if NET8_0_OR_GREATER
        if (clock is TimeProvider timeProvider)
        {
            functions = CacheFunctions.Create(timeProvider);
        }
#endif
        if (functions is null && clock is ISystemClock systemClock)
        {
            functions = CacheFunctions.Create(systemClock);
        }
        functions ??= CacheFunctions.Create();
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
        _cache = new FasterKV<SpanByte, SpanByte>(
            size: 1L << 20,
            logSettings: logSettings,
            checkpointSettings: new CheckpointSettings { CheckpointDir = path },
            comparer: new SpanByteComparer()
            );
    }

    internal bool IsDisposed { get; private set; }

    void IDisposable.Dispose()
    {
        IsDisposed = true;
        _cache.Dispose();
    }

    private bool Slide(ref SpanByte payload)
    {
        var span = payload.AsSpan();
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
    static bool LeaseFor(string key, [NotNullWhen(true)] out byte[]? oversized, out int length)
    {
        length = Encoding.GetByteCount(key);
        if (length <= MAX_STACKALLOC_SIZE)
        {
            // no need to check
            oversized = null;
            return false;
        }
        oversized = ArrayPool<byte>.Shared.Rent(length);
        return true;
    }

    static bool LeaseFor(byte[] value, [NotNullWhen(true)] out byte[]? oversized, out int length)
    {
        length = 12 + value.Length;
        if (length <= MAX_STACKALLOC_SIZE)
        {
            // no need to check
            oversized = null;
            return false;
        }
        oversized = ArrayPool<byte>.Shared.Rent(length);
        return true;
    }

    static void WriteKey(string key, Span<byte> buffer)
    {
        var len = Encoding.GetBytes(key, buffer);
        Debug.Assert(len == buffer.Length);
    }

    private void WriteValue(byte[] value, Span<byte> target, long absoluteExpiration, int slidingExpiration)
    {
        Debug.Assert(target.Length == value.Length + 12);
        BinaryPrimitives.WriteInt64LittleEndian(target, absoluteExpiration);
        BinaryPrimitives.WriteInt32LittleEndian(target.Slice(8), slidingExpiration);
        value.CopyTo(target.Slice(12));
    }

    unsafe byte[]? IDistributedCache.Get(string key)
    {
        Span<byte> s = LeaseFor(key, out var buffer, out var length) ? new(buffer, 0, length) : stackalloc byte[length];
        WriteKey(key, s);
        fixed (byte* ptr = s) // this might end up fixing the stack buffer; that's OK
        {
            var fixedKey = SpanByte.FromFixedSpan(s);
            using var session = _cache.For(_functions).NewSession<CacheFunctions>();
            var (status, payload) = session.Read(fixedKey);

            if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
            {
                if (Slide(ref payload)) // apply sliding expiration
                {
                    session.Upsert(ref fixedKey, ref payload);
                }
                if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
                return payload.ToByteArray();
            }
        }
        if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
        return null;
    }

    async Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        //var result = await session.ReadAsync(ref key, token: token);
        //var status = result.Status;
        //if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        //{
        //    var tmp = result.Output;
        //    if (Slide(ref tmp)) // apply sliding expiration
        //    {
        //        await session.UpsertAsync(ref key, ref tmp, token: token);
        //    }
        //    return result.Output.AsBytes();
        //}
        //return null;
    }

    unsafe void IDistributedCache.Refresh(string key)
    {
        Span<byte> s = LeaseFor(key, out var buffer, out var length) ? new(buffer, 0, length) : stackalloc byte[length];
        WriteKey(key, s);
        fixed (byte* ptr = s) // this might end up fixing the stack buffer; that's OK
        {
            var fixedKey = SpanByte.FromFixedSpan(s);
            using var session = _cache.For(_functions).NewSession<CacheFunctions>();
            var (status, payload) = session.Read(fixedKey);
            if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
            {
                if (Slide(ref payload)) // apply sliding expiration
                {
                    session.Upsert(ref fixedKey, ref payload);
                }
            }
        }
        if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
    }

    async Task IDistributedCache.RefreshAsync(string key, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        //var result = await session.ReadAsync(ref key, token: token);
        //var status = result.Status;
        //if (status.IsCompletedSuccessfully && status.Found && !status.Expired)
        //{
        //    var tmp = result.Output;
        //    if (Slide(ref tmp)) // apply sliding expiration
        //    {
        //        await session.UpsertAsync(ref key, ref tmp, token: token);
        //    }
        //}
    }

    unsafe void IDistributedCache.Remove(string key)
    {
        Span<byte> s = LeaseFor(key, out var buffer, out var length) ? new(buffer, 0, length) : stackalloc byte[length];
        WriteKey(key, s);
        fixed (byte* ptr = s) // this might end up fixing the stack buffer; that's OK
        {
            var fixedKey = SpanByte.FromFixedSpan(s);
            using var session = _cache.For(_functions).NewSession<CacheFunctions>();
            session.Delete(ref fixedKey);
        }
        if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
    }

    async Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        //await session.DeleteAsync(ref key, token: token);

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

    unsafe void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        Span<byte> s = LeaseFor(key, out var keyBuffer, out var keyBufferLength) ? new(keyBuffer, 0, keyBufferLength) : stackalloc byte[keyBufferLength];
        Span<byte> v = LeaseFor(value, out var valueBuffer, out var valueBufferLength) ? new(valueBuffer, 0, valueBufferLength) : stackalloc byte[valueBufferLength];

        WriteKey(key, s);
        WriteValue(value, v, GetExpiryTicks(options, out var sliding), sliding);

        fixed (byte* keyPtr = s) // this might end up fixing the stack buffer; that's OK
        fixed (byte* valuePtr = v) // this might end up fixing the stack buffer; that's OK
        {
            var fixedKey = SpanByte.FromFixedSpan(s);
            var fixedValue = SpanByte.FromFixedSpan(v);
            using var session = _cache.For(_functions).NewSession<CacheFunctions>();
            session.Upsert(ref fixedKey, ref fixedValue);
        }

        if (keyBuffer is not null) ArrayPool<byte>.Shared.Return(keyBuffer);
        if (valueBuffer is not null) ArrayPool<byte>.Shared.Return(valueBuffer);
    }

    async Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        await Task.Yield();
        throw new NotImplementedException();
        //Payload payload = new(GetExpiryTicks(options, out var sliding), sliding, value);
        //using var session = _cache.For(_functions).NewSession<CacheFunctions>();
        //await session.UpsertAsync(ref key, ref payload, token: token);
    }
}
