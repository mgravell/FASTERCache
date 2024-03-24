using FASTER.core;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace FASTERCache;

internal class StringSerializer : IObjectSerializer<string>
{
    private Stream _stream = Stream.Null;
    public void BeginDeserialize(Stream stream) => _stream = stream;

    public void BeginSerialize(Stream stream) => _stream = stream;

    public void Deserialize(out string obj)
    {
        Span<byte> header = stackalloc byte[4];
        int offset = 0, read;
        while (offset < 4 && (read = _stream.Read(header.Slice(offset))) > 0)
        {
            offset += read;
        }
        if (offset != 4) ThrowEOF();
        var len = BinaryPrimitives.ReadInt32LittleEndian(header);
        // be lazy for for
        var arr = ArrayPool<byte>.Shared.Rent(len);
        while (offset < len && (read = _stream.Read(arr, offset, len - offset)) > 0)
        {
            offset += read;
        }
        if (offset != len) ThrowEOF();
        obj = Encoding.GetString(arr, 0, len);
        ArrayPool<byte>.Shared.Return(arr);

        static void ThrowEOF() => throw new EndOfStreamException();

    }

    static readonly UTF8Encoding Encoding = new(false);

    public void EndDeserialize() => _stream = Stream.Null;

    public void EndSerialize() => _stream = Stream.Null;

    public void Serialize(ref string obj)
    {
        var len = Encoding.GetByteCount(obj);
        Span<byte> header = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(header, len);
        _stream.Write(header);
        
        var arr = ArrayPool<byte>.Shared.Rent(len);
        var actual = Encoding.GetBytes(obj, 0, obj.Length, arr, 0);
        Debug.Assert(actual == len);
        _stream.Write(arr, 0, len);
        ArrayPool<byte>.Shared.Return(arr);
    }
}
