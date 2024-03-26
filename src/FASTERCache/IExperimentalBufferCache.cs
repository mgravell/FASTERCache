using Microsoft.Extensions.Caching.Distributed;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;

namespace FASTERCache;

internal interface IExperimentalBufferCache : IDistributedCache
{
    Task<bool> GetAsync(string key, IBufferWriter<byte> target, CancellationToken token = default);
    Task SetAsync(string key, ReadOnlySequence<byte> value, DistributedCacheEntryOptions options, CancellationToken token = default);
}
