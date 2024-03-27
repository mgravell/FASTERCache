﻿using Microsoft.Extensions.Caching.Distributed;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace FASTERCache;

internal interface IExperimentalBufferCache : IDistributedCache
{
    ValueTask<bool> GetAsync(string key, IBufferWriter<byte> target, CancellationToken token = default);
    ValueTask SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token = default);

    bool Get(string key, IBufferWriter<byte> target);
    void Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options);
}
