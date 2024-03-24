using BenchmarkDotNet.Attributes;
#if NET8_0_OR_GREATER
using FusionRocks;
#endif
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using NeoSmart.Caching.Sqlite;
using NeoSmart.Caching.Sqlite.AspNetCore;
using System;
using System.Threading.Tasks;

namespace FASTERCache;

[SimpleJob, MemoryDiagnoser]
public class CacheBenchmarks : IDisposable
{
    private readonly IDistributedCache _faster, _sqlite;
#if NET8_0_OR_GREATER
    private readonly IDistributedCache _rocks;
#endif

    [Params(128)]
    public int KeyLength { get; set; } = 20;

    [Params(10 * 1024)]
    public int PayloadLength { get; set; } = 1024;

    private string key = "";
    private byte[] payload = [];

    [GlobalSetup]
    public void Init()
    {
        const string alphabet = @"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_!@#[]/\-+=";
        var rand = new Random();
        var arr = new byte[PayloadLength];
        rand.NextBytes(arr);
        payload = arr;
        var chars = new char[KeyLength];
        for (int i = 0; i < chars.Length; i++)
        {
            chars[i] = alphabet[rand.Next(0, alphabet.Length)];
        }
        key = new string(chars);

        _faster.Set(key, payload);
        _sqlite.Set(key, payload);
    }
    public CacheBenchmarks()
    {
        var services = new ServiceCollection();
        services.AddFASTERCache(options => options.Directory = "faster");
        _faster = services.BuildServiceProvider().GetRequiredService<IDistributedCache>();

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

    private int? Get(IDistributedCache cache) => cache.Get(key)?.Length;
    private async ValueTask<int?> GetAsync(IDistributedCache cache) => (await cache.GetAsync(key))?.Length;

    private void Set(IDistributedCache cache)
    {
        // scramble slightly each time
        payload[payload.Length / 2]++;
        cache.Set(key, payload);
    }
    private async Task SetAsync(IDistributedCache cache)
    {
        // scramble slightly each time
        payload[payload.Length / 2]++;
        await cache.SetAsync(key, payload);
    }


    [Benchmark]
    public int? FASTER_Get() => Get(_faster);

    [Benchmark]
    public void FASTER_Set() => Set(_faster);

    [Benchmark]
    public ValueTask<int?> FASTER_GetAsync() => GetAsync(_faster);

    [Benchmark]
    public Task FASTER_SetAsync() => SetAsync(_faster);

    [Benchmark]
    public int? SQLite_Get() => Get(_sqlite);

    [Benchmark]
    public void SQLite_Set() => Set(_sqlite);

    [Benchmark]
    public ValueTask<int?> SQLite_GetAsync() => GetAsync(_sqlite);

    [Benchmark]
    public Task SQLite_SetAsync() => SetAsync(_sqlite);

#if NET8_0_OR_GREATER
    [Benchmark]
    public int? Rocks_Get() => Get(_rocks);

    [Benchmark]
    public void Rocks_Set() => Set(_rocks);

    [Benchmark]
    public ValueTask<int?> Rocks_GetAsync() => GetAsync(_rocks);

    [Benchmark]
    public Task Rocks_SetAsync() => SetAsync(_rocks);
#endif
}
