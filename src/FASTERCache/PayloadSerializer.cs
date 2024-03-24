using FASTER.core;
using System;
using System.Buffers.Binary;
using System.IO;

namespace FASTERCache;

internal class PayloadSerializer : IObjectSerializer<Payload>
{
    private Stream _stream = Stream.Null;
    public void BeginDeserialize(Stream stream) => _stream = stream;

    public void BeginSerialize(Stream stream) => _stream = stream;

    public void Deserialize(out Payload obj)
    {
        Span<byte> header = stackalloc byte[20];
        int offset = 0, read;
        while (offset < header.Length && (read = _stream.Read(header.Slice(offset))) > 0)
        {
            offset += read;
        }
        if (offset != header.Length) ThrowEOF();

        var absolute = BinaryPrimitives.ReadInt64LittleEndian(header);
        var sliding = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(8));
        var len = BinaryPrimitives.ReadInt64LittleEndian(header.Slice(12));

        var arr = new byte[checked((int)len)];
        offset = 0;
        while (offset < arr.Length && (read = _stream.Read(arr, offset, arr.Length - offset)) > 0)
        {
            offset += read;
        }
        if (offset != arr.Length) ThrowEOF();
        obj = new(absolute, sliding, arr);
        static void ThrowEOF() => throw new EndOfStreamException();
    }

    public void EndDeserialize() => _stream = Stream.Null;

    public void EndSerialize() => _stream = Stream.Null;

    public void Serialize(ref Payload obj)
    {
        Span<byte> header = stackalloc byte[20];
        BinaryPrimitives.WriteInt64LittleEndian(header, obj.ExpiryTicks);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(8), obj.SlidingTicks);
        BinaryPrimitives.WriteInt64LittleEndian(header.Slice(12), obj.Value.Length);
        _stream.Write(header);
        if (obj.Value.IsSingleSegment)
        {
            _stream.Write(obj.Value.FirstSpan);
        }
        else
        {
            foreach (var chunk in obj.Value)
            {
                _stream.Write(chunk.Span);
            }
        }
    }
}
