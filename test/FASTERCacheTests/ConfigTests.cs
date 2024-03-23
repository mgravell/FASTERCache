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
        services.AddFASTERCache(options =>
        {
            options.Directory = "cachedir";
            configured = true;
        });
        FASTERDistributedCache typed;
        using (var provider = services.BuildServiceProvider())
        {
            var cache = provider.GetRequiredService<IDistributedCache>();
            typed = Assert.IsType<FASTERDistributedCache>(cache);
            Assert.True(configured);
        }
        Assert.True(typed.IsDisposed);
    }
}