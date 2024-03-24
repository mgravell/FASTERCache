using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using NeoSmart.Caching.Sqlite;

#if DEBUG
using var obj = new CacheBenchmarks();
obj.Init();
obj.FASTER_Set();
await obj.FASTER_SetAsync();
Console.WriteLine(obj.FASTER_Get());
Console.WriteLine(await obj.FASTER_GetAsync());
obj.SQLite_Set();
await obj.SQLite_SetAsync();
Console.WriteLine(obj.SQLite_Get());
Console.WriteLine(await obj.SQLite_GetAsync());

#else
BenchmarkRunner.Run<CacheBenchmarks>();
#endif

[SimpleJob, MemoryDiagnoser]
public class CacheBenchmarks : IDisposable
{
    private readonly IDistributedCache _faster, _sqlite;

    [Params(16, 512)]
    public int KeyLength { get; set; } = 20;

    [Params(128, 10 * 1024, 1024 * 1024)]
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
    }

    public void Dispose()
    {
        (_faster as IDisposable)?.Dispose();
        (_sqlite as IDisposable)?.Dispose();
    }

    [Benchmark]
    public int? FASTER_Get() => _faster.Get(key)?.Length;

    [Benchmark]
    public void FASTER_Set()
    {
        // scramble slightly each time
        payload[payload.Length / 2]++;
        _faster.Set(key, payload);
    }

    [Benchmark]
    public async ValueTask<int?> FASTER_GetAsync() => (await _faster.GetAsync(key))?.Length;

    [Benchmark]
    public async Task FASTER_SetAsync()
    {
        // scramble slightly each time
        payload[payload.Length / 2]++;
        await _faster.SetAsync(key, payload);
    }

    [Benchmark]
    public int? SQLite_Get() => _sqlite.Get(key)?.Length;

    [Benchmark]
    public void SQLite_Set()
    {
        // scramble slightly each time
        payload[payload.Length / 2]++;
        _sqlite.Set(key, payload);
    }

    [Benchmark]
    public async ValueTask<int?> SQLite_GetAsync() => (await _sqlite.GetAsync(key))?.Length;

    [Benchmark]
    public async Task SQLite_SetAsync()
    {
        // scramble slightly each time
        payload[payload.Length / 2]++;
        await _sqlite.SetAsync(key, payload);
    }
}