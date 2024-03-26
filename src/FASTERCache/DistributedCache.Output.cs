namespace FASTERCache;

partial class DistributedCache
{
    internal readonly struct Output
    {
        public Output(long absoluteExpiration, int slidingExpiration, byte[] payload)
        {
            AbsoluteExpiration = absoluteExpiration;
            SlidingExpiration = slidingExpiration;
            Payload = payload;
        }
        public readonly long AbsoluteExpiration;
        public readonly int SlidingExpiration;
        public readonly byte[]? Payload;
    }
}
