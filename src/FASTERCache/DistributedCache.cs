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

    ReadOnlySpan<byte> WriteValue(Span<byte> target, ReadOnlySequence<byte> value, out byte[] lease, DistributedCacheEntryOptions? options)
    {
        var absoluteExpiration = GetExpiryTicks(options, out var slidingExpiration);
        lease = EnsureSize(ref target, checked((int)value.Length + 12));
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


    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask<TResult?> AsyncTransitionGet<TResult>(ref GetAsyncState<TResult> state, int step)
    {
        return AsyncTransitionGetImpl(this, state, step);

        static async ValueTask<TResult?> AsyncTransitionGetImpl(
        DistributedCache @this, GetAsyncState<TResult> state, int step)
        {
            try
            {
                Output output;
                Status status;
                switch (step)
                {
                    case 0: goto CompletePendingRmw;
                    case 1: goto CompleteIncompleteRmw;
                    default: throw new ArgumentOutOfRangeException(nameof(step));
                }
            CompletePendingRmw:
                state.RmwResult = await state.PendingRmwResult;
                state.PendingRmwResult = IncorrectRwmState;
                status = state.RmwResult.Status;
                output = state.RmwResult.Output;
                if (!status.IsPending) goto RmwIsComplete;
            CompleteIncompleteRmw:
                // WHY NO CompletePendingAsync <== (from search? this one)
                // see https://github.com/microsoft/FASTER/issues/355#issuecomment-713213205
                // and https://github.com/microsoft/FASTER/issues/355#issuecomment-713204965
                // tl;dr: we should not need CompletePendingAsync
                // await state.Session.CompletePendingAsync(token: state.Token);
                (status, output) = state.RmwResult.Complete();
            RmwIsComplete:
                if (status.IsCompletedSuccessfully && status.Found)
                {
                    state.FinalResult = state.Selector(output);
                }
                Debug.WriteLine($"RMW: {status}");

                @this.ReuseSession(state.Session);
                return state.FinalResult;
            }
            catch
            {
                FaultSession(state.Session);
                throw;
            }
        }
    }

    static readonly ValueTask<FasterKV<SpanByte, SpanByte>.RmwAsyncResult<Input, Output, Empty>> IncorrectRwmState
        = ValueTask.FromException<FasterKV<SpanByte, SpanByte>.RmwAsyncResult<Input, Output, Empty>>(new InvalidOperationException("Incorrect RMW state"));

    
    static bool Force() => true;

    private struct GetAsyncState<TResult> // this exists mainly so we can have a cheap call-stack for AsyncTransitionPendingRead etc
    {
        public Input Input;
        public Func<Output, TResult> Selector;
        public CancellationToken Token;
        public ClientSession<SpanByte, SpanByte, Input, Output, Empty, CacheFunctions> Session;
        public TResult? FinalResult;
        public ValueTask<FasterKV<SpanByte, SpanByte>.RmwAsyncResult<Input, Output, Empty>> PendingRmwResult;
        public FasterKV<SpanByte, SpanByte>.RmwAsyncResult<Input, Output, Empty> RmwResult;
    }

    private unsafe ValueTask<TResult?> GetAsync<TResult>(string key, IBufferWriter<byte>? bufferWriter, bool readArray, Func<Output, TResult> selector, CancellationToken token)
    {
        var state = new GetAsyncState<TResult>();
        state.Input = new Input(readArray ? Input.OperationFlags.ReadArray : Input.OperationFlags.None, bufferWriter);
        state.Selector = selector;
        state.Token = token;
        state.Session = GetSession();
        state.FinalResult = default;
        state.RmwResult = default;
        state.PendingRmwResult = IncorrectRwmState;
        return GetAsyncImpl(key, ref state);
    }

    private unsafe ValueTask<TResult?> GetAsyncImpl<TResult>(string key, ref GetAsyncState<TResult> state)
    {
        try
        {
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            fixed (byte* keyPtr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                state.PendingRmwResult = state.Session.RMWAsync(fixedKey, state.Input, token: state.Token);
            }
            ReturnLease(lease);

            if (!state.PendingRmwResult.IsCompletedSuccessfully)
            {
                return AsyncTransitionGet(ref state, 0);
            }
            state.RmwResult = state.PendingRmwResult.GetAwaiter().GetResult();
            state.PendingRmwResult = IncorrectRwmState;

            var status = state.RmwResult.Status;
            var output = state.RmwResult.Output;
            if (status.IsPending)
            {
                return AsyncTransitionGet(ref state, 1);
            }

            if (status.IsCompletedSuccessfully && status.Found)
            {
                state.FinalResult = state.Selector(output);
                Debug.WriteLine($"Read: {status}");
            }
            ReuseSession(state.Session);
            return new(state.FinalResult);
        }
        catch
        {
            FaultSession(state.Session);
            throw;
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

            fixed (byte* keyPtr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);

                var input = new Input(readArray ? Input.OperationFlags.ReadArray : Input.OperationFlags.None, bufferWriter);
                Output output = default;
                var status = session.RMW(ref fixedKey, ref input, ref output);
                if (status.IsPending) CompleteSinglePending(session, ref status, ref output);
                if (status.IsCompletedSuccessfully && status.Found)
                {
                    finalResult = selector(output);
                }
                Debug.WriteLine($"RMW: {status}");
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
            fixed (byte* ptr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                var status = session.Delete(ref fixedKey);
                if (status.IsPending)
                {
                    Output dummy;
                    Unsafe.SkipInit(out dummy);
                    CompleteSinglePending(session, ref status, ref dummy);
                }
                Debug.WriteLine($"Delete: {status}");
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
        var keyLength = WriteKey(key, out var lease);
        var keyMemory = new Memory<byte>(lease, 0, keyLength);
        var session = GetSession();
        try
        {
            using (keyMemory.Pin())
            {
                var fixedKey = SpanByte.FromPinnedMemory(keyMemory);
                var result = await session.DeleteAsync(ref fixedKey, token: token);
                var status = result.Status;
                if (status.IsPending)
                {
                    // search: WHY NO CompletePendingAsync
                    // await session.CompletePendingAsync(token: token);
                    status = result.Complete();
                }
                Debug.WriteLine($"Delete: {status}");
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
            Debug.WriteLine("Write: " + BitConverter.ToString(valueSpan.Slice(12).ToArray()));
            fixed (byte* keyPtr = keySpan)
            fixed (byte* valuePtr = valueSpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                var fixedValue = SpanByte.FromFixedSpan(valueSpan);
                var status = session.Upsert(ref fixedKey, ref fixedValue);
                if (status.IsPending)
                {
                    Output dummy;
                    Unsafe.SkipInit(out dummy);
                    CompleteSinglePending(session, ref status, ref dummy);
                }
                Debug.WriteLine($"Upsert: {status}");
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
        => WriteAsync(key, new(value), options, token).AsTask();


    async ValueTask WriteAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        var keyLength = WriteKey(key, out var keyLease);
        var keyMemory = new Memory<byte>(keyLease, 0, keyLength);
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
                    // search: WHY NO CompletePendingAsync
                    // await session.CompletePendingAsync(token: token);
                    status = upsertResult.Complete();
                }
                Debug.WriteLine($"Upsert: {status}");
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

    ValueTask<bool> IExperimentalBufferCache.GetAsync(string key, IBufferWriter<byte> target, CancellationToken token)
        => GetAsync(key, bufferWriter: target, readArray: false, token: token, selector: static _ => true);

    ValueTask IExperimentalBufferCache.SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, value, options, token);

    bool IExperimentalBufferCache.Get(string key, IBufferWriter<byte> target)
        => Get(key, bufferWriter: target, readArray: false, selector: static _ => true);

    void IExperimentalBufferCache.Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
        => Write(key, value, options);
}
