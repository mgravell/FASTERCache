# TsavoriteCache

TsavoriteCache is an `IDistributedCache` implementation using Tsavorite, the storage and query engine
behind [Garnet](https://github.com/microsoft/Garnet) (a RESP server in .NET), providing a file-based
local cache that persists between application executions.

## Usage

``` c#
services.AddTsavoriteCache(options =>
{
    // optionally configure
    options.Settings = new(baseDir: "cache");
});
```

then just use `IDistributedCache` [as normal](https://learn.microsoft.com/aspnet/core/performance/caching/distributed).
