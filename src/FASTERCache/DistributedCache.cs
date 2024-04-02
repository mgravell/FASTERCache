using FASTER.core;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using BooleanSession = FASTER.core.ClientSession<FASTER.core.SpanByte, FASTER.core.SpanByte, FASTERCache.DistributedCache.BasicInputContext, bool, FASTER.core.Empty, FASTERCache.DistributedCache.BooleanFunctions>;
using ByteArraySession = FASTER.core.ClientSession<FASTER.core.SpanByte, FASTER.core.SpanByte, FASTERCache.DistributedCache.BasicInputContext, byte[], FASTER.core.Empty, FASTERCache.DistributedCache.ByteArrayFunctions>;

namespace FASTERCache;

/// <summary>
/// Implements IDistributedCache
/// </summary>
internal sealed partial class DistributedCache : CacheBase,
    IFASTERDistributedCache, IDisposable
{
    public bool SlidingExpiration { get; set; }
    protected override byte KeyPrefix => (byte)'D';

    // heavily influenced by https://github.com/microsoft/FASTER/blob/main/cs/samples/CacheStore/

    public DistributedCache(CacheService cacheService, IServiceProvider services, IOptions<FASTERCacheOptions> options)
        : this(options.Value, cacheService, GetClockObject(services))
    { }


    internal DistributedCache(FASTERCacheOptions options, CacheService cacheService, object? clock) : base(cacheService, clock)
    {
        SlidingExpiration = options.SlidingExpiration;
    }

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

    private readonly ConcurrentBag<ByteArraySession> _byteArraySessions = [];
    private readonly ConcurrentBag<BooleanSession> _booleanSessions = [];
    private BooleanSession GetBooleanSession() => GetSession(_booleanSessions, BooleanFunctions.Instance);

    private void ReuseSession(ByteArraySession session) => ReuseSession(_byteArraySessions, session);
    private void ReuseSession(BooleanSession session) => ReuseSession(_booleanSessions, session);

    const int MAX_STACKALLOC = 128;

    private ValueTask<TOutput?> GetAsync<TInput, TOutput, TFunction>(
        ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunction>> sessions,
        TFunction functions,
        string key, ref TInput input, CancellationToken cancellationToken)
        where TFunction : IFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        var session = GetSession(sessions, functions);
        try
        {
            TOutput? output = default!;
            Status status;
            var sliding = SlidingExpiration;
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            unsafe
            {
                fixed (byte* keyPtr = keySpan)
                {
                    var fixedKey = SpanByte.FromFixedSpan(keySpan);
                    status = sliding
                        ? session.RMW(ref fixedKey, ref input, ref output)
                        : session.Read(ref fixedKey, ref input, ref output);
                    DebugWipe(keySpan);
                }
            }
            ReturnLease(ref lease);
            if (status.IsPending)
            {
                return Awaited(this, session, sessions, cancellationToken);
            }

            Assert(status, sliding ? nameof(session.RMW) : nameof(session.Read));
            OnDebugRMWComplete(status, async: false);
            ReuseSession(sessions, session);
            return new(output);
        }
        catch
        {
            OnDebugFault();
            FaultSession(session);
            throw;
        }

        static async ValueTask<TOutput?> Awaited(DistributedCache @this, ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunction> session,
            ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunction>> sessions, CancellationToken token)
        {
            try
            {
                var outputs = await session.CompletePendingWithOutputsAsync(token: token);
                var status = CompleteSinglePending(outputs, out var output);

                Assert(status, @this.SlidingExpiration ? nameof(session.RMW) : nameof(session.Read));
                @this.OnDebugRMWComplete(status, async: true);
                ReuseSession(sessions, session);
                return output;
            }
            catch
            {
                @this.OnDebugFault();
                FaultSession(session);
                throw;
            }
        }
    }

    private unsafe TOutput? Get<TInput, TOutput, TFunction>(
    ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunction>> sessions,
    TFunction functions,
    string key, ref TInput input)
    where TFunction : IFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        var session = GetSession(sessions, functions);
        try
        {
            TOutput? output = default!;
            Status status;
            var sliding = SlidingExpiration;
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            fixed (byte* keyPtr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                status = sliding
                    ? session.RMW(ref fixedKey, ref input, ref output)
                    : session.Read(ref fixedKey, ref input, ref output);
                DebugWipe(keySpan);
            }
            ReturnLease(ref lease);
            if (status.IsPending)
            {
                status = CompleteSinglePending(session, out output);
            }

            Assert(status, sliding ? nameof(session.RMW) : nameof(session.Read));
            OnDebugRMWComplete(status, async: false);
            ReuseSession(sessions, session);
            return output;
        }
        catch
        {
            OnDebugFault();
            FaultSession(session);
            throw;
        }
    }

    private BasicInputContext BasicInput(IBufferWriter<byte>? target = null) => new(Clock.NowTicks, target);

    byte[]? IDistributedCache.Get(string key)
    {
        var input = BasicInput();
        return Get(_byteArraySessions, ByteArrayFunctions.Instance, key, ref input);
    }

    Task<byte[]?> IDistributedCache.GetAsync(string key, CancellationToken token) {
        var input = BasicInput();
        return GetAsync(_byteArraySessions, ByteArrayFunctions.Instance, key, ref input, token).AsTask();
    }

    unsafe void IDistributedCache.Refresh(string key)
    {
        var input = BasicInput();
        Get(_booleanSessions, BooleanFunctions.Instance, key, ref input);
    }

    Task IDistributedCache.RefreshAsync(string key, CancellationToken token)
    {
        var input = BasicInput();
        var pending = GetAsync(_booleanSessions, BooleanFunctions.Instance, key, ref input, token);
        if (pending.IsCompletedSuccessfully)
        {
            pending.GetAwaiter().GetResult(); // ensure consumed
            return Task.CompletedTask;
        }
        // error, async, etc
        return pending.AsTask();
    }

    unsafe void IDistributedCache.Remove(string key)
    {
        var session = GetBooleanSession();
        try
        {
            Status status;
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            fixed (byte* ptr = keySpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                status = session.Delete(ref fixedKey);
                DebugWipe(keySpan);
            }
            ReturnLease(ref lease);
            if (status.IsPending)
            {
                status = CompleteSinglePending(session, out _);
            }
            Assert(status, nameof(session.Delete));
            OnDebugRemoveComplete(status, false);
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
        var session = GetBooleanSession();
        try
        {
            Status status;
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var lease);
            unsafe
            {
                fixed (byte* ptr = keySpan)
                {
                    var fixedKey = SpanByte.FromFixedSpan(keySpan);
                    status = session.Delete(ref fixedKey);
                    DebugWipe(keySpan);
                }
            }
            ReturnLease(ref lease);
            if (status.IsPending)
            {
                return Awaited(this, session, token);
            }
            Assert(status, nameof(session.Delete));
            OnDebugRemoveComplete(status, false);
            ReuseSession(session);
            return Task.CompletedTask;
        }
        catch
        {
            FaultSession(session);
            throw;
        }

        static async Task Awaited(DistributedCache @this, BooleanSession session, CancellationToken token)
        {
            try
            {
                var outputs = await session.CompletePendingWithOutputsAsync(token: token);
                var status = CompleteSinglePending(outputs, out _);

                Assert(status, nameof(session.Delete));
                @this.OnDebugRemoveComplete(status, async: true);
                @this.ReuseSession(session);
            }
            catch
            {
                @this.OnDebugFault();
                FaultSession(session);
                throw;
            }
        }
    }

    private long GetExpiryTicks(DistributedCacheEntryOptions? options, out int sliding)
    {
        sliding = 0;
        var now = Clock.NowTicks;
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
                return now + options.AbsoluteExpirationRelativeToNow.GetValueOrDefault().Ticks;
            }
            if (sliding != 0)
            {
                return now + sliding;
            }
        }
        return now + OneMinuteTicks;
    }

    private static readonly long OneMinuteTicks = TimeSpan.FromMinutes(1).Ticks;

    unsafe void IDistributedCache.Set(string key, byte[] value, DistributedCacheEntryOptions options)
        => Write(key, new(value), options);

    unsafe void Write(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
    {
        var session = GetBooleanSession();
        try
        {
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var keyLease);
            var valueSpan = WriteValue(value.Length <= MAX_STACKALLOC - 12 ? stackalloc byte[MAX_STACKALLOC] : default,
                value, out var valueLease, options);

            Status status;
            fixed (byte* keyPtr = keySpan)
            fixed (byte* valuePtr = valueSpan)
            {
                var fixedKey = SpanByte.FromFixedSpan(keySpan);
                var fixedValue = SpanByte.FromFixedSpan(valueSpan);
                status = session.Upsert(ref fixedKey, ref fixedValue);
                DebugWipe(keySpan);
                DebugWipe(valueSpan);
            }
            ReturnLease(ref keyLease);
            ReturnLease(ref valueLease);

            if (status.IsPending)
            {
                status = CompleteSinglePending(session, out _);
            }
            Assert(status, nameof(session.Upsert));
            OnDebugUpsertComplete(status, false);
            ReuseSession(session);
        }
        catch
        {
            FaultSession(session);
            throw;
        }
    }

    ValueTask WriteAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
    {
        var session = GetBooleanSession();
        try
        {
            var keySpan = WriteKey(key.Length < MAX_STACKALLOC ? stackalloc byte[MAX_STACKALLOC] : default, key, out var keyLease);
            var valueSpan = WriteValue(value.Length <= MAX_STACKALLOC - 12 ? stackalloc byte[MAX_STACKALLOC] : default,
                value, out var valueLease, options);

            Status status;
            unsafe
            {
                fixed (byte* keyPtr = keySpan)
                fixed (byte* valuePtr = valueSpan)
                {
                    var fixedKey = SpanByte.FromFixedSpan(keySpan);
                    var fixedValue = SpanByte.FromFixedSpan(valueSpan);
                    status = session.Upsert(ref fixedKey, ref fixedValue);
                    DebugWipe(keySpan);
                    DebugWipe(valueSpan);
                }
            }
            ReturnLease(ref keyLease);
            ReturnLease(ref valueLease);

            if (status.IsPending)
            {
                return Awaited(this, session, token);
            }
            Assert(status, nameof(session.Upsert));
            OnDebugUpsertComplete(status, false);
            ReuseSession(session);
            return default;
        }
        catch
        {
            FaultSession(session);
            throw;
        }

        static async ValueTask Awaited(DistributedCache @this, BooleanSession session, CancellationToken token)
        {
            try
            {
                var outputs = await session.CompletePendingWithOutputsAsync(token: token);
                var status = CompleteSinglePending(outputs, out _);

                Assert(status, nameof(session.Upsert));
                @this.OnDebugUpsertComplete(status, async: true);
                @this.ReuseSession(session);
            }
            catch
            {
                @this.OnDebugFault();
                FaultSession(session);
                throw;
            }
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

    ValueTask<bool> IExperimentalBufferCache.GetAsync(string key, IBufferWriter<byte> target, CancellationToken token)
    {
        var input = BasicInput(target);
        return GetAsync(_booleanSessions, BooleanFunctions.Instance, key, ref input, token);
    }

    ValueTask IExperimentalBufferCache.SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token)
        => WriteAsync(key, value, options, token);

    bool IExperimentalBufferCache.Get(string key, IBufferWriter<byte> target)
    {
        var input = BasicInput(target);
        return Get(_booleanSessions, BooleanFunctions.Instance, key, ref input);
    }

    void IExperimentalBufferCache.Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options)
        => Write(key, value, options);


    private ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions>> GetSessionBag<TInput, TOutput, TFunctions>()
        where TFunctions : IFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
        where TInput : struct, IInputTime
    {
        var key = (typeof(TInput), typeof(TOutput), typeof(TFunctions));
        if (_bags.TryGetValue(key, out var found))
        {
            return (ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions>>)found;
        }
        
        var newObj = new ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions>>();
        _bags[key] = newObj;
        return newObj;
    }

    private readonly ConcurrentDictionary<(Type, Type, Type), object> _bags = new();


    readonly struct InPlaceReadInput<TState, TValue> : IInputTime
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0290:Use primary constructor", Justification = "Want to be very explicit about intent")]
        public InPlaceReadInput(long nowTicks, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer)
        {
            State = state;
            Deserializer = deserializer;
            NowTicks = nowTicks;
        }
        public long NowTicks { get; }
        public readonly TState State;
        public readonly Func<TState, ReadOnlySequence<byte>, TValue> Deserializer;

        public class Functions : CacheFunctions<InPlaceReadInput<TState, TValue>, TValue>
        {
            private Functions() { }
            public static readonly Functions Instance = new();
            protected override void Read(ref InPlaceReadInput<TState, TValue> input, ReadOnlySpan<byte> payload, ref TValue output)
            {
                var mgr = UnsafeMemoryManager.Create(payload);
                output = input.Deserializer(input.State, new(mgr.Memory));
                mgr.Return();
            }
        }
    }

    public unsafe class UnsafeMemoryManager : MemoryManager<byte>
    {
        private static readonly ConcurrentBag<UnsafeMemoryManager> spares = [];
        private UnsafeMemoryManager() { }
        public static UnsafeMemoryManager Create(ReadOnlySpan<byte> fixedSpan)
        {
            if (!spares.TryTake(out var found)) found = new();
            found.ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(fixedSpan));
            found.length = fixedSpan.Length;
            return found;
        }
        public void Return()
        {
            const int APPROX_MAX_SPARES = 32;
            length = 0;
            ptr = null;
            if (spares.Count < APPROX_MAX_SPARES)
            {
                spares.Add(this);
            }
        }
        private void* ptr;
        private int length;
        public override Span<byte> GetSpan() => MemoryMarshal.CreateSpan(ref Unsafe.AsRef<byte>(ptr), length);
        protected override void Dispose(bool disposing) { }
        public override MemoryHandle Pin(int elementIndex = 0) => new((byte*)ptr + elementIndex);
        public override void Unpin() { }

    }
    TValue? IFASTERDistributedCache.Get<TState, TValue>(string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer) where TValue : default
    {
        var sessions = GetSessionBag<InPlaceReadInput<TState, TValue>, TValue, InPlaceReadInput<TState, TValue>.Functions>();
        var input = new InPlaceReadInput<TState, TValue>(Clock.NowTicks, in state, deserializer);
        return Get(sessions, InPlaceReadInput<TState, TValue>.Functions.Instance, key, ref input);
    }

    ValueTask<TValue?> IFASTERDistributedCache.GetAsync<TState, TValue>(string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer, CancellationToken token) where TValue : default
    {
        var sessions = GetSessionBag<InPlaceReadInput<TState, TValue>, TValue, InPlaceReadInput<TState, TValue>.Functions>();
        var input = new InPlaceReadInput<TState, TValue>(Clock.NowTicks, in state, deserializer);
        return GetAsync(sessions, InPlaceReadInput<TState, TValue>.Functions.Instance, key, ref input, token);
    }
}
