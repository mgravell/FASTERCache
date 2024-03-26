using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTERCache;

public class FunctionalTests(FunctionalTests.CacheInstance fixture) : IClassFixture<FunctionalTests.CacheInstance>
{
    private IDistributedCache Cache => fixture.Cache;
    private IExperimentalBufferCache BufferCache => fixture.BufferCache;

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
            services.AddFASTERDistributedCache(options => options.Directory = "cachedir");
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

    private static string Caller([CallerMemberName] string caller = "") => caller;

    [Fact]
    public void BasicUsage()
    {
        var key = Caller();
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
    }

    [Fact]
    public async Task BasicUsageAsync()
    {
        var key = Caller();
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
    }

    private static readonly DistributedCacheEntryOptions RelativeFiveMinutes = new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) };

    private static readonly DistributedCacheEntryOptions SlidingOneMinute = new() { SlidingExpiration = TimeSpan.FromMinutes(1) };

    [Fact]
    public void RelativeExpiration()
    {
        var key = Caller();
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
    }

    [Fact]
    public async Task RelativeExpirationAsync()
    {
        var key = Caller();
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
    }

    [Fact]
    public void SlidingExpiration()
    {
        var key = Caller();
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
    }

    [Fact]
    public async Task SlidingExpirationAsync()
    {
        var key = Caller();
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
    }

}
