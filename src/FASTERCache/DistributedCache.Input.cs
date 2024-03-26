using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System;

namespace FASTERCache;

partial class DistributedCache
{
    internal readonly struct Input
    {
        public Input(OperationFlags flags, IBufferWriter<byte>? writer = null)
        {
            Flags = flags;
            Writer = writer;
            AbsoluteExpiration = 0;
        }
        private Input(in Input source, long absoluteExpiration)
        {
            Flags = source.Flags | OperationFlags.WriteSlide;
            Writer = source.Writer;
            AbsoluteExpiration = absoluteExpiration;
        }

        public readonly IBufferWriter<byte>? Writer;
        public readonly OperationFlags Flags;
        public readonly long AbsoluteExpiration;
        public bool ReadToArray => (Flags & OperationFlags.ReadArray) != 0;
        public bool ReadToWriter
        {
            [MemberNotNullWhen(true, nameof(Writer))]
            get => Writer is not null;
        }
        public bool WriteSlide => (Flags & OperationFlags.WriteSlide) != 0;

        public override string ToString() => Flags.ToString();

        internal Input Slide(long absoluteExpiration)
            => new Input(in this, absoluteExpiration);

        [Flags]
        public enum OperationFlags
        {
            None = 0,
            ReadArray = 1 << 0,
            WriteSlide = 1 << 1,
        }
    }
}
