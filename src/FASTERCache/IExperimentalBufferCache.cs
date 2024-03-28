using Microsoft.Extensions.Caching.Distributed;
using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace FASTERCache;

#pragma warning disable RS0016 // Add public types and members to the declared API - this type needs to go into runtime
public interface IExperimentalBufferCache : IDistributedCache
{
    ValueTask<bool> GetAsync(string key, IBufferWriter<byte> target, CancellationToken token = default);
    ValueTask SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token = default);

    bool Get(string key, IBufferWriter<byte> target);
    void Set(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options);
}
#pragma warning restore RS0016 // Add public types and members to the declared API

public interface IFASTERDistributedCache : IExperimentalBufferCache
{
    TValue? Get<TState, TValue>(string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer);
    ValueTask<TValue?> GetAsync<TState, TValue>(string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer, CancellationToken token = default);
}