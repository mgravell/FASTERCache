using FASTER.core;
using System.Buffers;
using System.IO;

namespace FASTERCache;

internal class BLOBSerializer : BinaryObjectSerializer<ReadOnlySequence<byte>>
{
    public override void Deserialize(out ReadOnlySequence<byte> obj)
    {
        var len = reader.ReadInt32();
        if (len == 0)
        {
            obj = default;
            return;
        }
        var arr = new byte[len];
        int offset = 0, count;
        while (len > 0 && (count = reader.Read(arr, offset, len - offset)) > 0)
        {
            offset += count;
            len -= count;
        }
        if (len != 0) throw new EndOfStreamException();
        obj = new(arr);
    }

    public override void Serialize(ref ReadOnlySequence<byte> obj)
    {
        writer.Write(checked((int)obj.Length));
        if (obj.IsSingleSegment)
        {
            writer.Write(obj.FirstSpan);
        }
        else
        {
            foreach (var chunk in obj)
            {
                writer.Write(chunk.Span);
            }
        }
    }
}
