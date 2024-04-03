using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FASTERCache;

public class FunctionalTests : IClassFixture<FunctionalTests.CacheInstance>
{
    public FunctionalTests(CacheInstance fixture, ITestOutputHelper log)
    {
        this.log = log;
        this.fixture = fixture;
        ResetStats();
    }
    private readonly CacheInstance fixture;
    private readonly ITestOutputHelper log;
    private IDistributedCache Cache => fixture.Cache;
    private IExperimentalBufferCache BufferCache => fixture.BufferCache;
    private const int PageSizeBits = 12;

    public sealed class CacheInstance : IDisposable
    {
        private readonly ServiceProvider provider;
        private readonly IDistributedCache cache;

        private readonly TestTimeProvider time = new();
        public void SetTime(DateTimeOffset value) => time.Set(value);
        public void AddTime(TimeSpan value) => time.Add(value);
        public IDistributedCache Cache => cache;
        internal IExperimentalBufferCache BufferCache => (IExperimentalBufferCache)cache;
        public CacheInstance()
        {
            var services = new ServiceCollection();
#if NET8_0_OR_GREATER
            services.AddSingleton<TimeProvider>(time);
#else
            services.AddSingleton<ISystemClock>(time);
#endif
            services.AddFASTERDistributedCache(options =>
            {
                options.Directory = "cachedir";
                options.LogSettings.PageSizeBits = PageSizeBits;
                options.LogSettings.MemorySizeBits = options.LogSettings.PageSizeBits + 2;
                options.LogSettings.SegmentSizeBits = options.LogSettings.MemorySizeBits + 2;
                options.DeleteOnClose = true;
            });
            provider = services.BuildServiceProvider();
            cache = provider.GetRequiredService<IDistributedCache>();
        }
        public void Dispose() => provider.Dispose();
    }

    public sealed class TestTimeProvider
#if NET8_0_OR_GREATER
        : TimeProvider
#else
        : ISystemClock
#endif
    {
        private DateTimeOffset now = DateTime.UtcNow;
        public void Set(DateTimeOffset value) => now = value;
        public void Add(TimeSpan value) => now += value;
#if NET8_0_OR_GREATER
        public override DateTimeOffset GetUtcNow() => now;
#else
        DateTimeOffset ISystemClock.UtcNow => now;
#endif
    }

    private static string Caller(bool sliding, [CallerMemberName] string caller = "") => $"{caller}:{sliding}";

    static readonly Func<int, ReadOnlySequence<byte>, string> Deserializer = static (s, payload) => s.ToString() + ":" + Encoding.UTF8.GetString(payload);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void InPlaceRead(bool sliding)
    {
        SetSliding(sliding);
        var key = Caller(sliding);
        Assert.Null(Cache.Get(key));

        var faster = Assert.IsAssignableFrom<IFASTERDistributedCache>(Cache);
        string? s = faster.Get(key, 42, Deserializer);
        Assert.Null(s);

        var original = Guid.NewGuid().ToString();
        Cache.Set(key, Encoding.UTF8.GetBytes(original));
        s = faster.Get(key, 42, Deserializer);
        Assert.Equal("42:" + original, s);
        WriteStats();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BasicUsage(bool sliding)
    {
        SetSliding(sliding);
        var key = Caller(sliding);
        Assert.Null(Cache.Get(key));
        var original = Guid.NewGuid().ToByteArray();
        Cache.Set(key, original);

        var retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        Cache.Refresh(key);
        retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        Cache.Remove(key);
        Assert.Null(Cache.Get(key));
        WriteStats();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BasicUsageAsync(bool sliding)
    {
        SetSliding(sliding);
        var key = Caller(sliding);
        var bw = new ArrayBufferWriter<byte>();
        Assert.Null(await Cache.GetAsync(key));
        Assert.False(await BufferCache.GetAsync(key, bw));
        Assert.Equal(0, bw.WrittenCount);

        var original = Guid.NewGuid().ToByteArray();
        await Cache.SetAsync(key, original);

        var retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        await Cache.RefreshAsync(key);
        retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        Assert.True(await BufferCache.GetAsync(key, bw));
        Assert.Equal(original, bw.WrittenSpan.ToArray());

        await Cache.RemoveAsync(key);
        Assert.Null(await Cache.GetAsync(key));
        WriteStats();
    }

    private void SetSliding(bool sliding)
    {
        if (Cache is DistributedCache dc)
        {
            dc.SlidingExpiration = sliding;
        }
    }

    private static readonly DistributedCacheEntryOptions RelativeFiveMinutes = new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) };

    private static readonly DistributedCacheEntryOptions RelativeNever = new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromDays(1000) };

    private static readonly DistributedCacheEntryOptions SlidingOneMinute = new() { SlidingExpiration = TimeSpan.FromMinutes(1) };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RelativeExpiration(bool sliding)
    {
        SetSliding(sliding);
        var key = Caller(sliding);
        Assert.Null(Cache.Get(key));
        var original = Guid.NewGuid().ToByteArray();
        Cache.Set(key, original, RelativeFiveMinutes);

        var retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(4.8));
        retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(0.4));
        retrieved = Cache.Get(key);
        Assert.Null(retrieved);
        WriteStats();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RelativeExpirationAsync(bool sliding)
    {
        SetSliding(sliding);
        var key = Caller(sliding);
        Assert.Null(await Cache.GetAsync(key));
        var original = Guid.NewGuid().ToByteArray();
        await Cache.SetAsync(key, original, RelativeFiveMinutes);

        var retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(4.8));
        retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(0.4));
        retrieved = await Cache.GetAsync(key);
        Assert.Null(retrieved);
        WriteStats();
    }

    [Fact]
    public void SlidingExpiration()
    {
        SetSliding(true);
        var key = Caller(sliding: true);
        Assert.Null(Cache.Get(key));
        var original = Guid.NewGuid().ToByteArray();
        Cache.Set(key, original, SlidingOneMinute);

        var retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        for (int i = 0; i < 5; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            retrieved = Cache.Get(key);
            Assert.NotNull(retrieved);
            Assert.Equal(original, retrieved);
        }

        for (int i = 0; i < 3; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            Cache.Refresh(key);
        }
        fixture.AddTime(TimeSpan.FromMinutes(0.8));
        retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(1.2));
        retrieved = Cache.Get(key);
        Assert.Null(retrieved);
        WriteStats();
    }

    [Fact]
    public void SlidingExpirationForceDisk()
    {
        var key = Caller(true);
        Assert.Null(Cache.Get(key));
        var original = Guid.NewGuid().ToByteArray();

        // memory size is PageSizeBits+2
        //init with trash, up to the memory->disk transition size
        byte[] trashData = new byte[1 << (PageSizeBits - 2)];
        Random.Shared.NextBytes(trashData);
        List<string> trashKeys = [];
        for (int i = 0; i < 16; i++)
        {
            var trashKey = Guid.NewGuid().ToString("N");
            trashKeys.Add(trashKey);
            Cache.Set(trashKey, trashData, RelativeNever);
        }

        //init with trash, up to the memory->disk transition size
        foreach (var trashKey in Enumerable.Reverse(trashKeys))
        {
            var trash = Cache.Get(trashKey);
            Assert.NotNull(trash);
        }

        Cache.Set(key, original, SlidingOneMinute);

        var retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        for (int i = 0; i < 5; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            retrieved = Cache.Get(key);
            Assert.NotNull(retrieved);
            Assert.Equal(original, retrieved);
        }

        for (int i = 0; i < 3; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            Cache.Refresh(key);
        }

        fixture.AddTime(TimeSpan.FromMinutes(0.8));
        retrieved = Cache.Get(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(1.2));
        retrieved = Cache.Get(key);
        Assert.Null(retrieved);

        foreach (var trashKey in trashKeys)
        {
            retrieved = Cache.Get(trashKey);
            Assert.NotNull(retrieved);
        }
    }

    [Fact]
    public async Task SlidingExpirationAsync()
    {
        SetSliding(true);
        var key = Caller(sliding: true);
        Assert.Null(await Cache.GetAsync(key));
        var original = Guid.NewGuid().ToByteArray();
        await Cache.SetAsync(key, original, SlidingOneMinute);

        var retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        for (int i = 0; i < 5; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            retrieved = await Cache.GetAsync(key);
            Assert.NotNull(retrieved);
            Assert.Equal(original, retrieved);
        }

        for (int i = 0; i < 3; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            await Cache.RefreshAsync(key);
        }
        fixture.AddTime(TimeSpan.FromMinutes(0.8));
        retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(1.2));
        retrieved = await Cache.GetAsync(key);
        Assert.Null(retrieved);
        WriteStats();
    }

    [Fact]
    public async Task SlidingExpirationForceDiskAsync()
    {
        SetSliding(true);
        var key = Caller(sliding: true);
        Assert.Null(await Cache.GetAsync(key));
        var original = Guid.NewGuid().ToByteArray();
        // memory size is PageSizeBits+2
        //init with trash, up to the memory->disk transition size
        byte[] trashData = new byte[1 << (PageSizeBits - 2)]; 
        Random.Shared.NextBytes(trashData);
        List<string> trashKeys = [];
        for (int i = 0; i < 16; i++)
        {
            var trashKey = Guid.NewGuid().ToString("N");
            trashKeys.Add(trashKey);
            await Cache.SetAsync(trashKey, trashData, RelativeNever);
        }

        // reversed to simplify tracking keys
        foreach (var trashKey in Enumerable.Reverse(trashKeys))
        {
            var trash = await Cache.GetAsync(trashKey);
            Assert.NotNull(trash);
        }

        await Cache.SetAsync(key, original, SlidingOneMinute);

        var retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        for (int i = 0; i < 5; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            retrieved = await Cache.GetAsync(key);
            Assert.NotNull(retrieved);
            Assert.Equal(original, retrieved);
        }

        for (int i = 0; i < 3; i++)
        {
            fixture.AddTime(TimeSpan.FromMinutes(0.8));
            await Cache.RefreshAsync(key);
        }
        fixture.AddTime(TimeSpan.FromMinutes(0.8));
        retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        fixture.AddTime(TimeSpan.FromMinutes(1.2));
        retrieved = await Cache.GetAsync(key);
        Assert.Null(retrieved);
        foreach (var trashKey in trashKeys)
        {
            retrieved = await Cache.GetAsync(trashKey);
            Assert.NotNull(retrieved);
        }
        WriteStats();
    }
    private void ResetStats()
    {
#if DEBUG
        if (Cache is DistributedCache dc) dc.ResetCounters();
#endif
    }
    private void WriteStats()
    {
#if DEBUG
        if (Cache is DistributedCache dc)
        {
            log.WriteLine($"Hit: {dc.TotalHit}");
            log.WriteLine($"Miss: {dc.TotalMiss} (of which {dc.TotalMissExpired} were expired)");
            log.WriteLine($"Sync: {dc.TotalSync}");
            log.WriteLine($"Async: {dc.TotalAsync}");
            log.WriteLine($"Copy-Update: {dc.TotalCopyUpdate}");
            log.WriteLine($"Other: {dc.TotalOther}");
            log.WriteLine($"Fault: {dc.TotalFault}");
        }
#endif
    }

    [Fact]
    public async Task LargerThanMemory()
    {
        // deleteonclose is true, this assumes a somehow clean cachedir folder
        var cacheFolder = Path.Join(Environment.CurrentDirectory, "cachedir");
        Assert.True(Directory.Exists(cacheFolder));
        var files = Directory.GetFiles(cacheFolder, "*.log*");
        int inititalCount = files.Length;
        byte[] data = new byte[1 << (PageSizeBits - 1)]; // 32KiB segment size, pagesize is 4KiB, writing 2KiB
        Random.Shared.NextBytes(data);
        List<string> keys = [];
        for (int i = 0; i < 64; i++)
        {
            var key = Guid.NewGuid().ToString("N");
            keys.Add(key);
            await Cache.SetAsync(key, data, RelativeNever);
        }

        // just make sure we can load them again
        foreach (var key in keys)
        {
            var retrieved = await Cache.GetAsync(key);
            Assert.NotNull(retrieved);
        }

        // we should have now around 4 files more than before, just test >, due to FS delay
        files = Directory.GetFiles(cacheFolder, "*.log*");
        Assert.NotEmpty(files);
        int afterSetCount = files.Length;
        Assert.True(afterSetCount > inititalCount);
    }
}
