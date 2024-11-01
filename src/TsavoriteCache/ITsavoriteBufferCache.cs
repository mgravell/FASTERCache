using Microsoft.Extensions.Caching.Distributed;
using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace TsavoriteCache;

#pragma warning disable RS0016 // Add public types and members to the declared API - this type needs to go into runtime

[Experimental("FCACHE001")]
public interface ITsavoriteDistributedCache : IBufferDistributedCache // allows in-place deserialize
{
    TValue? Get<TState, TValue>(string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer);
    ValueTask<TValue?> GetAsync<TState, TValue>(string key, in TState state, Func<TState, ReadOnlySequence<byte>, TValue> deserializer, CancellationToken token = default);
}
#pragma warning restore RS0016 // Add public types and members to the declared API