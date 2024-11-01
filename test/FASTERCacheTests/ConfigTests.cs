using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace FASTERCache;

public class ConfigTests
{
    [Fact]
    public void CanCreate()
    {
        var services = new ServiceCollection();
        bool configured = false;
        services.AddFASTERDistributedCache(options =>
        {
            options.Settings = new("cachedir", deleteDirOnDispose: true);
            configured = true;
        });
        DistributedCache typed;
        using (var provider = services.BuildServiceProvider())
        {
            var cache = provider.GetRequiredService<IDistributedCache>();
            typed = Assert.IsType<DistributedCache>(cache);
            Assert.True(configured);
        }
        Assert.True(typed.IsDisposed);
    }

    [Fact]
    public void CanCreateWithBuilder()
    {
        var cache = new FASTERCacheBuilder(new("dummy", deleteDirOnDispose: true)).CreateDistributedCache();
        DistributedCache typed;
        using (cache as IDisposable)
        {
            typed = Assert.IsType<DistributedCache>(cache);
            Assert.False(typed.IsDisposed);
        }
        Assert.True(typed.IsDisposed);
    }
}