using FASTER.core;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace FASTERCache;

/// <summary>
/// Base class for cache service implementations, for example "output cache" or "distributed cache";
/// the KeyPrefix disambiguates keyspaces to prevent conflicts
/// </summary>
internal abstract class CacheBase : IDisposable
{
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
    private protected abstract void RemoveRef();
    protected static UTF8Encoding Encoding = new(false);

    protected static void FaultSession(IDisposable session)
    {
        try
        {
            session?.Dispose();
        }
        catch { } // we already expect trouble; don't make it worse
    }

    protected static void ReturnLease(ref byte[]? lease)
    {
        if (lease is not null)
        {
            ArrayPool<byte>.Shared.Return(lease);
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
}
internal abstract class CacheBase<TInput, TOutput, TContext, TFunctions> : CacheBase
    where TFunctions : IFunctions<SpanByte, SpanByte, TInput, TOutput, TContext>
{

    private protected sealed override void RemoveRef() => Cache.RemoveRef();
    protected readonly CacheService Cache;
    protected readonly TFunctions Functions;

    private readonly ConcurrentBag<ClientSession<SpanByte, SpanByte, TInput, TOutput, TContext, TFunctions>> _clientSessions = [];

    public CacheBase(CacheService cacheService, TFunctions functions)
    {
        Functions = functions;
        Cache = cacheService;
        Cache.AddRef();
    }

    protected static void CompleteSinglePending(ClientSession<SpanByte, SpanByte, TInput, TOutput, TContext, TFunctions> session, ref Status status, ref TOutput output)
    {
        if (!session.CompletePendingWithOutputs(out var outputs, wait: true)) Throw();
        int count = 0;
        while (outputs.Next())
        {
            ref CompletedOutput<SpanByte, SpanByte, TInput, TOutput, TContext> current = ref outputs.Current;
            status = current.Status;
            output = current.Output;
            count++;
        }
        if (count != 1) Throw();

        static void Throw() => throw new InvalidOperationException("Exactly one pending operation was expected");
    }


    protected ClientSession<SpanByte, SpanByte, TInput, TOutput, TContext, TFunctions> GetSession()
       => _clientSessions.TryTake(out var session) ? session : Cache.CreateSession<TInput, TOutput, TContext, TFunctions>(Functions);

    protected void ReuseSession(
        ClientSession<SpanByte, SpanByte, TInput, TOutput, TContext, TFunctions> session)
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
}
