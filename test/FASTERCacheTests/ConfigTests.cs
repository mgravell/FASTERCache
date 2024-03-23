using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;

namespace FASTERCache;

public class ConfigTests
{
    [Fact]
    public void CanCreate()
    {
        var services = new ServiceCollection();
        bool configured = false;
        services.AddFASTERCache(_ => configured = true);
        var cache = services.BuildServiceProvider().GetRequiredService<IDistributedCache>();
        Assert.IsType<FASTERDistributedCache>(cache);
        Assert.True(configured);

    }
}