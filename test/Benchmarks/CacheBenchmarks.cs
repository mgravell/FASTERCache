using BenchmarkDotNet.Attributes;
#if NET8_0_OR_GREATER
using FusionRocks;
#endif
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using NeoSmart.Caching.Sqlite;
using NeoSmart.Caching.Sqlite.AspNetCore;
using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace FASTERCache;

[SimpleJob, MemoryDiagnoser, MaxIterationCount(15), MinIterationCount(1)]
public class CacheBenchmarks : IDisposable
{
    private readonly IDistributedCache _faster, _sqlite;
    private readonly IExperimentalBufferCache _fasterBuffer;
#if NET8_0_OR_GREATER
    private readonly IDistributedCache _rocks;
#endif

    [Params(true, false)]
    public bool Sliding { get; set; } = true;

    [Params(128)]
    public int KeyLength { get; set; } = 20;

    [Params(10 * 1024)]
    public int PayloadLength { get; set; } = 1024;

    private string key = "";
    private Memory<byte> payload = default; // we intentionally want to show impact of needing write-sized array on write path

    [GlobalSetup]
    public void Init()
    {
        const string alphabet = @"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_!@#[]/\-+=";
        var rand = new Random();
        var arr = new byte[PayloadLength + 128];
        rand.NextBytes(arr);
        payload = new (arr, 0, PayloadLength);
        var chars = new char[KeyLength];
        for (int i = 0; i < chars.Length; i++)
        {
            chars[i] = alphabet[rand.Next(0, alphabet.Length)];
        }
        key = new string(chars);

        var finalArr = payload.ToArray();
        _faster.Set(key, finalArr);
        _sqlite.Set(key, finalArr);
#if NET8_0_OR_GREATER
        _rocks.Set(key, finalArr);
#endif
        if (_faster is DistributedCache dc)
        {
            dc.SlidingExpiration = Sliding;
        }
    }
    public CacheBenchmarks()
    {
        var services = new ServiceCollection();
        services.AddFASTERDistributedCache(options => options.Directory = "faster");
        _faster = services.BuildServiceProvider().GetRequiredService<IDistributedCache>();
        _fasterBuffer = (IExperimentalBufferCache)_faster;

        services = new ServiceCollection();
        services.AddSqliteCache(options => options.CachePath = @"sqlite.db", null!);
        _sqlite = services.BuildServiceProvider().GetRequiredService<IDistributedCache>();

#if NET8_0_OR_GREATER
        _rocks = new FusionRocks.FusionRocks(new FusionRocksOptions { CachePath = "rocks" });
#endif
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
        (_faster as IDisposable)?.Dispose();
        (_sqlite as IDisposable)?.Dispose();
#if NET8_0_OR_GREATER
        (_rocks as IDisposable)?.Dispose();
#endif
    }

    private int Get(IDistributedCache cache) => Assert(cache.Get(key));
    private int GetNew(IExperimentalBufferCache cache)
    {
        using var bw = CountingBufferWriter.Create();
        return Assert(cache.Get(key, bw) ? bw.Count : -1);
    }
    private async ValueTask<int> GetAsync(IDistributedCache cache) => Assert(await cache.GetAsync(key));

    internal sealed class CountingBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private static int _instanceCount;
        public static int InstanceCount => Volatile.Read(ref _instanceCount);
        private CountingBufferWriter() => Interlocked.Increment(ref _instanceCount);
        private static CountingBufferWriter? _spare;
        public static CountingBufferWriter Create() => Interlocked.Exchange(ref _spare, null) ?? new();

        // note that the memcopy is happening here - it isn't an unfair test

        private byte[] _buffer = ArrayPool<byte>.Shared.Rent(1000);
        public int Count { get;private set; }

        public void Advance(int count) => Count += count;

        public void Dispose()
        {
            Count = 0;
            if (Interlocked.CompareExchange(ref _spare, this, null) != null)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
            }
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => _buffer;

        public Span<byte> GetSpan(int sizeHint = 0) => _buffer;
    }
    private ValueTask<int> GetAsyncNew(IExperimentalBufferCache cache)
    {
        var bw = CountingBufferWriter.Create();
        var pending = cache.GetAsync(key, bw);
        if (!pending.IsCompletedSuccessfully) return Awaited(this, pending, bw);
        
        int count = pending.GetAwaiter().GetResult() ? bw.Count : -1;
        bw.Dispose();
        return new(Assert(count));

        static async ValueTask<int> Awaited(CacheBenchmarks @this, ValueTask<bool> pending, CountingBufferWriter bw)
        {
            using (bw)
            {
                return @this.Assert(await pending ? bw.Count : -1);
            }
        }
    }

    private int Assert(int length)
    {
        if (length != PayloadLength) Throw();
        return length;
        static void Throw() => throw new InvalidOperationException("incorrect payload");
    }
    private int Assert(byte[]? payload)
    {
        if (payload is null) Throw();
        if (payload.Length != PayloadLength) Throw();
        return payload.Length;

        [DoesNotReturn]
        static void Throw() => throw new InvalidOperationException("incorrect payload");
    }

    private void Set(IDistributedCache cache)
    {
        // scramble slightly each time
        payload.Span[payload.Length / 2]++;
        cache.Set(key, payload.ToArray(), Expiry);
    }

    private void SetNew(IExperimentalBufferCache cache)
    {
        // scramble slightly each time
        payload.Span[payload.Length / 2]++;
        ReadOnlySequence<byte> ros = new(payload);
        cache.Set(key, ros, Expiry);
    }

    private async Task SetAsync(IDistributedCache cache)
    {
        // scramble slightly each time
        payload.Span[payload.Length / 2]++;
        await cache.SetAsync(key, payload.ToArray(), Expiry);
    }

    private static DistributedCacheEntryOptions Expiry = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1) };

    private async Task SetAsyncNew(IExperimentalBufferCache cache)
    {
        // scramble slightly each time
        payload.Span[payload.Length / 2]++;
        ReadOnlySequence<byte> ros = new(payload);
        await cache.SetAsync(key, ros, Expiry);
    }

    [Benchmark]
    public int FASTER_Get() => Get(_faster);
    
    [Benchmark]
    public void FASTER_Set() => Set(_faster);

    [Benchmark]
    public int FASTER_GetBuffer() => GetNew(_fasterBuffer);

    [Benchmark]
    public void FASTER_SetBuffer() => SetNew(_fasterBuffer);

    [Benchmark]
    public ValueTask<int> FASTER_GetAsync() => GetAsync(_faster);
    [Benchmark]
    public ValueTask<int> FASTER_GetAsyncBuffer() => GetAsyncNew(_fasterBuffer);

    [Benchmark]
    public Task FASTER_SetAsync() => SetAsync(_faster);
    [Benchmark]
    public Task FASTER_SetAsyncBuffer() => SetAsyncNew(_fasterBuffer);

    [Benchmark]
    public int SQLite_Get() => Get(_sqlite);

    [Benchmark]
    public void SQLite_Set() => Set(_sqlite);

    [Benchmark]
    public ValueTask<int> SQLite_GetAsync() => GetAsync(_sqlite);

    [Benchmark]
    public Task SQLite_SetAsync() => SetAsync(_sqlite);

#if NET8_0_OR_GREATER
    [Benchmark]
    public int Rocks_Get() => Get(_rocks);

    [Benchmark]
    public void Rocks_Set() => Set(_rocks);

    [Benchmark]
    public ValueTask<int> Rocks_GetAsync() => GetAsync(_rocks);

    [Benchmark]
    public Task Rocks_SetAsync() => SetAsync(_rocks);
#endif
}
