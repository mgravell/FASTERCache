using FASTER.core;
using System.IO;

namespace FASTERCache;

internal class StringSerializer : BinaryObjectSerializer<string>
{
    // we can do better here, I'm sure
    public override void Deserialize(out string obj) => obj = reader.ReadString();

    public override void Serialize(ref string obj) => writer.Write(obj);
}
