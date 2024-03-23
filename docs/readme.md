# FASTERCache

FASTERCache is an `IDistributedCache` implementation using [FASTER](https://github.com/microsoft/FASTER),
providing a file-based local cache that persists between application executions.

## Usage

``` c#
services.AddFASTERCache(option => {
    // configure
});
```

then just use `IDistributedCache` [as normal](https://learn.microsoft.com/aspnet/core/performance/caching/distributed).