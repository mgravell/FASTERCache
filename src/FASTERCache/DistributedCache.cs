using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTERCache;

/// <summary>
/// Implements IDistributedCache
/// </summary>
internal sealed partial class DistributedCache : CacheBase<DistributedCache.Input, DistributedCache.Output, Empty, DistributedCache.CacheFunctions>, IDistributedCache, IDisposable, IExperimentalBufferCache
{
    protected override byte KeyPrefix => (byte)'D';

    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    public DistributedCache(CacheService cacheService, IServiceProvider services)
        : this(cacheService, GetClock(services))
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

    internal DistributedCache(CacheService cacheService, object? clock) : base(cacheService, clock switch
    {
#if NET8_0_OR_GREATER
        TimeProvider timeProvider => CacheFunctions.Create(timeProvider),
#endif
        ISystemClock systemClock => CacheFunctions.Create(systemClock),
        _ => CacheFunctions.Create()
    })
    { }

    const int MAX_STACKALLOC_SIZE = 128;

    ReadOnlySpan<byte> WriteValue(Span<byte> target, ReadOnlySequence<byte> value, out byte[]? lease, DistributedCacheEntryOptions? options)
    {
        var absoluteExpiration = GetExpiryTicks(options, out var slidingExpiration);
        lease = EnsureSize(ref target, checked((int)value.Length + 12));
        BinaryPrimitives.WriteInt64LittleEndian(target.Slice(0, 8), absoluteExpiration);
        BinaryPrimitives.WriteInt32LittleEndian(target.Slice(8, 4), slidingExpiration);
        value.CopyTo(target.Slice(12));
        return target;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsReadHit(byte status)
    {
        const byte BasicMask = 0xF, Expired = 0x80;

        return (status & (BasicMask | Expired)) == 0;
        /*
        what we want here is: status.IsCompletedSuccessfully && status.Found && !status.Expired,
        but obviously, in one hit

        IsCompletedSuccessfully:
            StatusCode statusCode = this.statusCode & StatusCode.BasicMask;
            if (statusCode != StatusCode.Pending)
            {
                return statusCode != StatusCode.Error;
            }
        Found:
            (Record.statusCode & StatusCode.BasicMask) == 0;
        Expired:
            (statusCode & StatusCode.Expired) == StatusCode.Expired;

        so: Found already excludes pending/error, which is nice; that leaves Expired
        */
    }

    static bool Force() => true;

    private ValueTask<TResult?> GetAsync<TResult>(string key, IBufferWriter<byte>? bufferWriter, bool readArray, Func<Output, TResult> selector, CancellationToken token)
    {
        var session = GetSession();
        try
        {
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            ValueTask<FasterKV<SpanByte, SpanByte>.RmwAsyncResult<Input, Output, Empty>> pendingRmwResult;
            unsafe
            {
                fixed (byte* keyPtr = keySpan)
                {
                    var fixedKey = SpanByte.FromFixedSpan(keySpan);
                    var input = new Input(readArray ? Input.OperationFlags.ReadArray : Input.OperationFlags.None, bufferWriter);
                    pendingRmwResult = session.RMWAsync(ref fixedKey, ref input, token: token);
                }
            }
            ReturnLease(ref lease);

            if (!pendingRmwResult.IsCompletedSuccessfully)
            {
                return Awaited(this, session, pendingRmwResult, selector);
            }
            var rmwResult = pendingRmwResult.GetAwaiter().GetResult();

            var status = rmwResult.Status;
            var output = rmwResult.Output;
            if (status.IsPending)
            {
                // WHY NO CompletePendingAsync <== (from search? this one)
                // see https://github.com/microsoft/FASTER/issues/355#issuecomment-713213205
                // and https://github.com/microsoft/FASTER/issues/355#issuecomment-713204965
                // tl;dr: we should not need CompletePendingAsync
                // await state.Session.CompletePendingAsync(token: state.Token);
                (status, output) = rmwResult.Complete();
            }
            Assert(status, nameof(session.RMWAsync));
            OnDebugReadComplete(status, async: false);
            TResult? finalResult = IsReadHit(status.Value) ? selector(output) : default;
            ReuseSession(session);
            return new(finalResult);
        }
        catch
        {
            OnDebugFault();
            FaultSession(session);
            throw;
        }
        static async ValueTask<TResult?> Awaited(DistributedCache @this,
            ClientSession<SpanByte, SpanByte, Input, Output, Empty, CacheFunctions> session,
            ValueTask<FasterKV<SpanByte, SpanByte>.RmwAsyncResult<Input, Output, Empty>> pendingRmwResult,
            Func<Output, TResult> selector
            )
        {
            try
            {
                var rmwResult = await pendingRmwResult;

                var status = rmwResult.Status;
                var output = rmwResult.Output;
                if (status.IsPending)
                {
                    // WHY NO CompletePendingAsync <== (from search? this one)
                    // see https://github.com/microsoft/FASTER/issues/355#issuecomment-713213205
                    // and https://github.com/microsoft/FASTER/issues/355#issuecomment-713204965
                    // tl;dr: we should not need CompletePendingAsync
                    // await state.Session.CompletePendingAsync(token: state.Token);
                    (status, output) = rmwResult.Complete();
                }
                Assert(status, nameof(session.RMWAsync));
                @this.OnDebugReadComplete(status, async: false);
                TResult? finalResult = IsReadHit(status.Value) ? selector(output) : default;
                @this.ReuseSession(session);
                return finalResult;
            }
            catch
            {
                @this.OnDebugFault();
                FaultSession(session);
                throw;
            }
        }
    }

    const int MAX_STACKALLOC = 128;

    private unsafe TResult? Get<TResult>(string key, bool readArray, Func<Output, TResult> selector, IBufferWriter<byte>? bufferWriter = null)
    {
        var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);

        TResult? finalResult = default;
        var session = GetSession();
        try
        {
            Status status;
            Output output = default;
            fixed (byte* keyPtr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                var input = new Input(readArray ? Input.OperationFlags.ReadArray : Input.OperationFlags.None, bufferWriter);
                status = session.RMW(ref fixedKey, ref input, ref output);
            }
            ReturnLease(ref lease);
            if (status.IsPending) CompleteSinglePending(session, ref status, ref output);
            Assert(status, nameof(session.RMW));

            OnDebugReadComplete(status, async: false);
            if (IsReadHit(status.Value))
            {
                finalResult = selector(output);
            }
            ReuseSession(session);
            return finalResult;
        }
        catch
        {
            OnDebugFault();
            FaultSession(session);
            throw;
        }
    }

    byte[]? IDistributedCache.Get(string key) => Get(key, readArray: true, selector: x => x.Payload);

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token) => GetAsync(key, bufferWriter: null, readArray: true, selector: static x => x.Payload, token: token).AsTask();

    unsafe void IDistributedCache.Refresh(string key) => Get(key, readArray: false, selector: x => true);

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token) => GetAsync(key, bufferWriter: null, readArray: false, selector: static _ => true, token: token).AsTask();

    unsafe void IDistributedCache.Remove(string key)
    {
        var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);

        var session = GetSession();
        try
        {
            Status status;
            fixed (byte* ptr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                status = session.Delete(ref fixedKey);
            }
            ReturnLease(ref lease);
            if (status.IsPending)
            {
                Output dummy;
                Unsafe.SkipInit(out dummy);
                CompleteSinglePending(session, ref status, ref dummy);
            }
            Assert(status, nameof(session.Delete));

            ReuseSession(session);
        }
        catch
        {
            FaultSession(session);
            throw;
        }
    }

    Task IDistributedCache.RemoveAsync(string key, CancellationToken token)
    {
        var session = GetSession();
        try
        {
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            ValueTask<FasterKV<SpanByte, SpanByte>.DeleteAsyncResult<Input, Output, Empty>> pendingDeleteResult;
            unsafe
            {
                fixed (byte* keyPtr = keySpan)
                {
                    var fixedKey = SpanByte.FromFixedSpan(keySpan);
                    pendingDeleteResult = session.DeleteAsync(ref fixedKey, token: token);
                }
            }
            ReturnLease(ref lease);

            if (!pendingDeleteResult.IsCompletedSuccessfully)
            {
                return Awaited(this, session, pendingDeleteResult);
            }
            var deleteResult = pendingDeleteResult.GetAwaiter().GetResult();
            var status = deleteResult.Status;
            if (status.IsPending)
            {
                // search: WHY NO CompletePendingAsync
                // await session.CompletePendingAsync(token: token);
                status = deleteResult.Complete();
            }
            Assert(status, nameof(session.DeleteAsync));
            ReuseSession(session);
            return Task.CompletedTask;
        }
        catch
        {
            FaultSession(session);
            throw;
        }

        static async Task Awaited(DistributedCache @this,
            ClientSession<SpanByte, SpanByte, Input, Output, Empty, CacheFunctions> session,
            ValueTask<FasterKV<SpanByte, SpanByte>.DeleteAsyncResult<Input, Output, Empty>> pendingDeleteResult)
        {
            try
            {
                var deleteResult = await pendingDeleteResult;
                var status = deleteResult.Status;
                if (status.IsPending)
                {
                    // search: WHY NO CompletePendingAsync
                    // await session.CompletePendingAsync(token: token);
                    status = deleteResult.Complete();
                }
                Assert(status, nameof(session.DeleteAsync));
                @this.ReuseSession(session);
            }
            catch
            {
                FaultSession(session);
                throw;
            }
        }

    }

    private long NowTicks => base.Functions.NowTicks;

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
        => Write(key, new(value), options);

    unsafe void Write(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
    {
        var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var keyLease);
        var valueSpan = WriteValue(value.Length <= MAX_STACKALLOC - 12 ? stackalloc byte[MAX_STACKALLOC] : default,
            value, out var valueLease, options);

        var session = GetSession();
        try
        {
            Status status;
            fixed (byte* keyPtr = keySpan)
            fixed (byte* valuePtr = valueSpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                var fixedValue = SpanByte.FromFixedSpan(valueSpan);
                status = session.Upsert(ref fixedKey, ref fixedValue);
            }
            ReturnLease(ref keyLease);
            ReturnLease(ref valueLease);

            if (status.IsPending)
            {
                Output dummy;
                Unsafe.SkipInit(out dummy);
                CompleteSinglePending(session, ref status, ref dummy);
            }
            Assert(status, nameof(session.Upsert));
            ReuseSession(session);
        }
        catch
        {
            FaultSession(session);
            throw;
        }
    }
    static void Assert(Status status, string method)
    {
        Debug.WriteLine($"{method}: {status}");
        if (status.IsFaulted) Throw(method);
        static void Throw(string method) => throw new InvalidOperationException("FASTER call faulted: " + method);
    }
    Task IDistributedCache.SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, new(value), options, token).AsTask();


    ValueTask WriteAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        var session = GetSession();
        try
        {
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            var valueSpan = WriteValue(value.Length <= MAX_STACKALLOC - 12 ? stackalloc byte[MAX_STACKALLOC] : default, value, out var valueLease, options);
            ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<Input, Output, Empty>> pendingUpsertResult;
            unsafe
            {
                fixed (byte* keyPtr = keySpan)
                fixed (byte* valuePtr = valueSpan)
                {
                    var fixedKey = SpanByte.FromFixedSpan(keySpan);
                    var fixedValue = SpanByte.FromFixedSpan(valueSpan);
                    pendingUpsertResult = session.UpsertAsync(ref fixedKey, ref fixedValue, token: token);
                }
            }
            ReturnLease(ref lease);
            ReturnLease(ref valueLease);

            if (!pendingUpsertResult.IsCompletedSuccessfully)
            {
                return Awaited(this, session, pendingUpsertResult);
            }
            var upsertResult = pendingUpsertResult.GetAwaiter().GetResult();
            var status = upsertResult.Status;
            if (status.IsPending)
            {
                // search: WHY NO CompletePendingAsync
                // await session.CompletePendingAsync(token: token);
                status = upsertResult.Complete();
            }
            Assert(status, nameof(session.UpsertAsync));
            ReuseSession(session);
            return default;
        }
        catch
        {
            FaultSession(session);
            throw;
        }

        static async ValueTask Awaited(DistributedCache @this,
            ClientSession<SpanByte, SpanByte, Input, Output, Empty, CacheFunctions> session,
            ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<Input, Output, Empty>> pendingUpsertResult
            )
        {
            try
            {
                var upsertResult = await pendingUpsertResult;
                var status = upsertResult.Status;
                if (status.IsPending)
                {
                    // search: WHY NO CompletePendingAsync
                    // await session.CompletePendingAsync(token: token);
                    status = upsertResult.Complete();
                }
                Assert(status, nameof(session.UpsertAsync));
                @this.ReuseSession(session);
            }
            catch
            {
                FaultSession(session);
                throw;
            }
        }
    }

    ValueTask<bool> IExperimentalBufferCache.GetAsync(string key, IBufferWriter<byte> target, CancellationToken token)
        => GetAsync(key, bufferWriter: target, readArray: false, token: token, selector: static _ => true);

    ValueTask IExperimentalBufferCache.SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, value, options, token);

    bool IExperimentalBufferCache.Get(string key, IBufferWriter<byte> target)
        => Get(key, bufferWriter: target, readArray: false, selector: static _ => true);

    void IExperimentalBufferCache.Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
        => Write(key, value, options);
}
