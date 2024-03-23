using FASTER.core;
using System.Buffers;
using System.IO;

namespace FASTERCache;

internal class PayloadSerializer : BinaryObjectSerializer<Payload>
{
    public override void Deserialize(out Payload obj)
    {
        var expiry = reader.ReadInt64();
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
        obj = new(expiry, arr);
    }

    public override void Serialize(ref Payload obj)
    {
        writer.Write(obj.ExpiryTicks);
        writer.Write(checked((int)obj.Value.Length));
        if (obj.Value.IsSingleSegment)
        {
            writer.Write(obj.Value.FirstSpan);
        }
        else
        {
            foreach (var chunk in obj.Value)
            {
                writer.Write(chunk.Span);
            }
        }
    }
}
