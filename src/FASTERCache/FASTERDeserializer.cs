using System;

namespace FASTERCache;
public delegate TValue Deserializer<TState, TValue>(in TState state, bool found, ReadOnlySpan<byte> payload);
