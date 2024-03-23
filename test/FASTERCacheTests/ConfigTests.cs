using Microsoft.Extensions.DependencyInjection;

namespace FASTERCacheTests;

public class ConfigTests
{
    [Fact]
    public void CanCreate()
    {
        var services = new ServiceCollection();
        services.AddFASTERCache(options =>
        {

        });
    }
}