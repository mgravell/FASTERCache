using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Internal;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTERCache;

public class FunctionalTests : IClassFixture<FunctionalTests.CacheInstance>
{
    private readonly CacheInstance fixture;
    private IDistributedCache Cache => fixture.Cache;
    public FunctionalTests(CacheInstance fixture) => this.fixture = fixture;
    public class CacheInstance : IDisposable
    {
        private readonly ServiceProvider provider;
        private readonly IDistributedCache cache;

        private readonly TestTimeProvider time = new();
        public void SetTime(DateTimeOffset value) => time.Set(value);
        public void AddTime(TimeSpan value) => time.Add(value);
        public IDistributedCache Cache => cache;
        public CacheInstance()
        {
            var services = new ServiceCollection();
#if NET8_0_OR_GREATER
            services.AddSingleton<TimeProvider>(time);
#else
            services.AddSingleton<ISystemClock>(time);
#endif
            services.AddFASTERCache(options => options.Directory = "cachedir");
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
        Assert.Null(await Cache.GetAsync(key));
        var original = Guid.NewGuid().ToByteArray();
        await Cache.SetAsync(key, original);

        var retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        await Cache.RefreshAsync(key);
        retrieved = await Cache.GetAsync(key);
        Assert.NotNull(retrieved);
        Assert.Equal(original, retrieved);

        await Cache.RemoveAsync(key);
        Assert.Null(await Cache.GetAsync(key));
    }

    DistributedCacheEntryOptions fiveMinutes = new DistributedCacheEntryOptions { AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) };

    DistributedCacheEntryOptions slidingOneMinute = new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(1) };

    [Fact]
    public void RelativeExpiration()
    {
        var key = Caller();
        Assert.Null(Cache.Get(key));
        var original = Guid.NewGuid().ToByteArray();
        Cache.Set(key, original, fiveMinutes);

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
        await Cache.SetAsync(key, original, fiveMinutes);

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
        Cache.Set(key, original, slidingOneMinute);

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
        await Cache.SetAsync(key, original, slidingOneMinute);

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
