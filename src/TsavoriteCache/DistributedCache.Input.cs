using System.Buffers;

namespace TsavoriteCache;

partial class DistributedCache
{
    internal readonly struct BasicInputContext : IInputTime
    {
        public BasicInputContext(long now, IBufferWriter<byte>? target = null)
        {
            NowTicks = now;
            Target = target;
        }
        public long NowTicks { get; }
        public readonly IBufferWriter<byte>? Target;
    }
}
