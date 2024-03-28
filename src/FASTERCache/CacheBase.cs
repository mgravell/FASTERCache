using FASTER.core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace FASTERCache;

/// <summary>
/// Base class for cache service implementations, for example "output cache" or "distributed cache";
/// the KeyPrefix disambiguates keyspaces to prevent conflicts
/// </summary>
internal abstract class CacheBase : IDisposable
{
    public CacheBase(CacheService cacheService, object? clock)
    {
        Cache = cacheService;
        Clock = clock switch
        {
            Clock typed => typed,
#if NET8_0_OR_GREATER
            TimeProvider timeProvider => Clock.Create(timeProvider),
#endif
            ISystemClock systemClock => Clock.Create(systemClock),
            null => Clock.Create(),
            _ => throw new ArgumentException("Unexpected clock type: " + clock.GetType().Name, nameof(clock)),
        };
        Cache.AddRef();
    }

    private int _isDisposed;
    internal bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;
    protected abstract byte KeyPrefix { get; }
    void IDisposable.Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            RemoveRef();
        }
    }
    private protected void RemoveRef() => Cache.RemoveRef();
    protected static UTF8Encoding Encoding = new(false);

    protected static void FaultSession(IDisposable session)
    {
        try
        {
            session?.Dispose();
        }
        catch { } // we already expect trouble; don't make it worse
    }

    protected static void ReuseSession<TSession>(
        ConcurrentBag<TSession> sessions,
        TSession session) where TSession : class, IDisposable
    {
        const int MAX_APPROX_SESSIONS = 20;
        if (sessions.Count <= MAX_APPROX_SESSIONS) // note race, that's fine
        {
            sessions.Add(session);
        }
        else
        {
            session.Dispose();
        }
    }

    protected static void ReturnLease(ref byte[]? lease)
    {
        if (lease is not null)
        {
            ArrayPool<byte>.Shared.Return(lease, clearArray: false);
            lease = null; // prevent double-return
        }
    }

    protected static byte[]? EnsureSize(ref Span<byte> target, int length)
    {
        if (length > target.Length)
        {
            var arr = ArrayPool<byte>.Shared.Rent(length);
            target = new(arr, 0, length);
            return arr;
        }
        target = target.Slice(0, length);
        return null;
    }
    protected ReadOnlySpan<byte> WriteKey(Span<byte> target, string key, out byte[]? lease)
    {
        var length = Encoding.GetByteCount(key) + 1;
        lease = EnsureSize(ref target, length);
        target[0] = KeyPrefix;
        var actualLength = Encoding.GetBytes(key, target.Slice(1));
        Debug.Assert(length == actualLength + 1);
        return target;
    }

    internal static object? GetClockObject(IServiceProvider services)
    {
        if (services is null) return null;
#if NET8_0_OR_GREATER
        if (services.GetService<TimeProvider>() is { } time)
        {
            return time;
        }
#endif
        return services.GetService<ISystemClock>();
    }

    protected readonly CacheService Cache;
    protected readonly Clock Clock;


    protected ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions> GetSession<TInput, TOutput, TFunctions>(
        ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions>> sessions,
        TFunctions functions
    )
        where TFunctions : IFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
        => sessions.TryTake(out var session) ? session : Cache.CreateSession<TInput, TOutput, Empty, TFunctions>(functions);

    protected static void CompleteSinglePending<TInput, TOutput, TFunctions>(ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions> session, ref Status status, ref TOutput output)
        where TFunctions : IFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        if (!session.CompletePendingWithOutputs(out var outputs, wait: true)) Throw();
        int count = 0;
        while (outputs.Next())
        {
            ref CompletedOutput<SpanByte, SpanByte, TInput, TOutput, Empty> current = ref outputs.Current;
            status = current.Status;
            output = current.Output;
            count++;
        }
        if (count != 1) Throw();

        static void Throw() => throw new InvalidOperationException("Exactly one pending operation was expected");
    }
}

internal abstract class Clock
{
    public abstract long NowTicks { get; }

#if NET8_0_OR_GREATER
    private sealed class TimeProviderClock : Clock
    {
        private static TimeProviderClock? s_shared;
        internal static TimeProviderClock Shared => s_shared ??= new(TimeProvider.System);
        public TimeProviderClock(TimeProvider time) => _time = time;
        private readonly TimeProvider _time;
        public override long NowTicks => _time.GetUtcNow().UtcTicks;
    }
    internal static Clock Create(TimeProvider time) => new TimeProviderClock(time);
    internal static Clock Create() => TimeProviderClock.Shared;
#else
        internal static Clock Create() => SystemClockClock.Shared;
#endif
    internal static Clock Create(ISystemClock time) => new SystemClockClock(time);
    private sealed class SystemClockClock : Clock
    {
#if !NET8_0_OR_GREATER
        private static SystemClockClock? s_shared;
        internal static SystemClockClock Shared => s_shared ??= new(new SystemClock());
#endif
        public SystemClockClock(ISystemClock time) => _time = time;
        private readonly ISystemClock _time;
        public override long NowTicks => _time.UtcNow.UtcTicks;
    }
}
