using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace FASTERCache;

internal readonly struct Payload
{
    public Payload(long absoluteTicks, int slidingTicks, byte[] value)
    {
        SlidingTicks = slidingTicks;
        ExpiryTicks = absoluteTicks;
        Value = new(value);
    }
    public Payload(long expiryTicks, int slidingTicks, ReadOnlySequence<byte> value)
    {
        SlidingTicks = slidingTicks;
        ExpiryTicks = expiryTicks;
        Value = value;
    }

    internal Payload(in Payload source, long expiryTicks)
    {
        SlidingTicks = source.SlidingTicks;
        Value = source.Value;
        ExpiryTicks = expiryTicks;
    }

    public readonly int SlidingTicks;
    public readonly long ExpiryTicks;
    public readonly DateTime ExpiryDateTime => new DateTime(ExpiryTicks);
    public readonly ReadOnlySequence<byte> Value;

    internal byte[]? AsBytes()
    {
        if (Value.IsSingleSegment)
        {
            var first = Value.First;
            if (first.IsEmpty) return [];
            if (MemoryMarshal.TryGetArray(first, out var segment)
                && segment.Array is not null && segment.Offset == 0
                && segment.Count == segment.Array.Length)
            {
                return segment.Array;
            }
        }
        return Value.ToArray();
    }

    internal Payload WithExpiry(long after)
    {
        throw new NotImplementedException();
    }
}
