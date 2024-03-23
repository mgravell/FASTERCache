using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace FASTERCache;

internal readonly struct Payload
{
    public Payload(long expiryTicks, byte[] value)
    {
        ExpiryTicks = expiryTicks;
        Value = new(value);
    }
    public Payload(DateTime expiry, byte[] value)
    {
        ExpiryTicks = expiry.Ticks;
        Value = new(value);
    }
    public Payload(DateTime expiry, ReadOnlySequence<byte> value)
    {
        ExpiryTicks = expiry.Ticks;
        Value = value;
    }
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
}
