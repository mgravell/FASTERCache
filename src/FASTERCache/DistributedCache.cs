using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
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


    private bool Slide(in Output output, ref Input input)
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

    const int MAX_STACKALLOC_SIZE = 128;

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
                    case 0: goto CompletePendingRead;
                    case 1: goto CompleteIncompleteRead;
                    case 2: goto CompletePendingUpsert;
                    case 3: goto CompleteIncompleteUpsert;
                    default: throw new ArgumentOutOfRangeException(nameof(step));
                }
            CompletePendingRead:
                state.ReadResult = await state.PendingReadResult;
                state.PendingReadResult = IncorrectReadState;
                status = state.ReadResult.Status;
                output = state.ReadResult.Output;
                if (!status.IsPending) goto ReadIsComplete;
                CompleteIncompleteRead:
                await state.Session.CompletePendingAsync(token: state.Token);
                (status, output) = state.ReadResult.Complete();
            ReadIsComplete:
                if (!(status.IsCompletedSuccessfully && status.Found))
                    goto AfterSlide;

                state.FinalResult = state.Selector(output);
                Debug.WriteLine($"Read: {status}");

                if (!@this.Slide(output, ref state.Input))
                    goto AfterSlide;

                unsafe
                {
                    fixed (byte* keyPtr = state.KeyLease)
                    {
                        // https://github.com/microsoft/FASTER/issues/444#issuecomment-809737698
                        // "Data only needs to be fixed until the operation comes back from the initial call. When an operation goes
                        // pending, we copy the data to an internal buffer before returning control to you, so you no longer need it fixed."
                        var fixedKey = SpanByte.FromPointer(keyPtr, state.KeyLength);
                        state.PendingUpsertResult = state.Session.UpsertAsync(fixedKey, state.Input, default, token: state.Token);
                    }
                }

            CompletePendingUpsert:
                state.UpsertResult = await state.PendingUpsertResult;
                state.PendingUpsertResult = IncorrectUpsertState;
                status = state.UpsertResult.Status;
                if (!status.IsPending) goto UpsertIsComplete;
                CompleteIncompleteUpsert:
                await state.Session.CompletePendingAsync(token: state.Token);
                status = state.UpsertResult.Complete();
            UpsertIsComplete:
                Debug.WriteLine($"Upsert (slide): {status}");
            AfterSlide:
                ReturnLease(state.KeyLease); // doesn't matter that still pinned
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

    static readonly ValueTask<FasterKV<SpanByte, SpanByte>.ReadAsyncResult<Input, Output, Empty>> IncorrectReadState
        = ValueTask.FromException<FasterKV<SpanByte, SpanByte>.ReadAsyncResult<Input, Output, Empty>>(new InvalidOperationException("Incorrect read state"));

    static readonly ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<Input, Output, Empty>> IncorrectUpsertState
        = ValueTask.FromException<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<Input, Output, Empty>>(new InvalidOperationException("Incorrect upsert state"));

    static bool Force() => true;

    private struct GetAsyncState<TResult> // this exists mainly so we can have a cheap call-stack for AsyncTransitionPendingRead etc
    {
        public int KeyLength;
        public byte[] KeyLease;
        public Input Input;
        public Func<Output, TResult> Selector;
        public CancellationToken Token;
        public ClientSession<SpanByte, SpanByte, Input, Output, Empty, CacheFunctions> Session;
        public TResult? FinalResult;
        public ValueTask<FasterKV<SpanByte, SpanByte>.ReadAsyncResult<Input, Output, Empty>> PendingReadResult;
        public FasterKV<SpanByte, SpanByte>.ReadAsyncResult<Input, Output, Empty> ReadResult;
        public ValueTask<FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<Input, Output, Empty>> PendingUpsertResult;
        public FasterKV<SpanByte, SpanByte>.UpsertAsyncResult<Input, Output, Empty> UpsertResult;
    }

    private unsafe ValueTask<TResult?> GetAsync<TResult>(string key, IBufferWriter<byte>? bufferWriter, bool readArray, Func<Output, TResult> selector, CancellationToken token)
    {
        var state = new GetAsyncState<TResult>();
        state.KeyLength = WriteKey(key, out state.KeyLease);
        state.Input = new Input(readArray ? Input.OperationFlags.ReadArray : Input.OperationFlags.None, bufferWriter);
        state.Selector = selector;
        state.Token = token;
        state.Session = GetSession();
        state.FinalResult = default;
        state.ReadResult = default;
        state.UpsertResult = default;
        state.PendingReadResult = IncorrectReadState;
        state.PendingUpsertResult = IncorrectUpsertState;
        return GetAsyncImpl(ref state);
    }

    private unsafe ValueTask<TResult?> GetAsyncImpl<TResult>(ref GetAsyncState<TResult> state)
    {
        try
        {
            fixed (byte* keyPtr = state.KeyLease)
            {
                var fixedKey = SpanByte.FromPointer(keyPtr, state.KeyLength);

                state.PendingReadResult = state.Session.ReadAsync(fixedKey, state.Input, token: state.Token);
                if (!state.PendingReadResult.IsCompletedSuccessfully)
                {
                    return AsyncTransitionGet(ref state, 0);
                }
                state.ReadResult = state.PendingReadResult.GetAwaiter().GetResult();
                state.PendingReadResult = IncorrectReadState;

                var status = state.ReadResult.Status;
                var output = state.ReadResult.Output;
                if (status.IsPending)
                {
                    return AsyncTransitionGet(ref state, 1);
                }

                if (status.IsCompletedSuccessfully && status.Found)
                {
                    state.FinalResult = state.Selector(output);
                    Debug.WriteLine($"Read: {status}");

                    if (Slide(output, ref state.Input))
                    {
                        state.PendingUpsertResult = state.Session.UpsertAsync(fixedKey, state.Input, default, token: state.Token);
                        if (!state.PendingUpsertResult.IsCompletedSuccessfully)
                        {
                            return AsyncTransitionGet(ref state, 2);
                        }
                        state.UpsertResult = state.PendingUpsertResult.GetAwaiter().GetResult();
                        state.PendingUpsertResult = IncorrectUpsertState;

                        status = state.UpsertResult.Status;
                        if (status.IsPending)
                        {
                            return AsyncTransitionGet(ref state, 3);
                        }
                        Debug.WriteLine($"Upsert (slide): {status}");
                    }
                }
            }
            ReturnLease(state.KeyLease);
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

                var input = new Input(getPayload ? Input.OperationFlags.ReadArray : Input.OperationFlags.None);
                Output output = default;
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

    byte[]? IDistributedCache.Get(string key) => Get(key, getPayload: true);

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token) => GetAsync(key, bufferWriter: null, readArray: true, selector: static x => x.Payload, token: token).AsTask();

    unsafe void IDistributedCache.Refresh(string key) => Get(key, getPayload: false);

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token) => GetAsync(key, bufferWriter: null, readArray: false, selector: static _ => true, token: token).AsTask();

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
                    await session.CompletePendingAsync(token: token);
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
                    await session.CompletePendingAsync(token: token);
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
        => GetAsync(key, bufferWriter: target, readArray: false, token: token, selector: static x => true);

    ValueTask IExperimentalBufferCache.SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, value, options, token);
}
