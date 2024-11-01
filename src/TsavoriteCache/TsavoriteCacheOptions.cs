using Tsavorite.core;
using Microsoft.Extensions.Options;
using System.IO;

namespace TsavoriteCache;

/// <summary>
/// Allows configuration of Tsavorite cache stores; these settings can be shared between
/// multiple cache implementations - for example "Output Cache" and "Distributed Cache"
/// might share a Tsavorite instance.
/// </summary>
public sealed class TsavoriteCacheOptions
{
    public KVSettings<SpanByte, SpanByte>? Settings { get; set; }

    public bool SlidingExpiration { get; set; } = true;

    internal class Validator : IValidateOptions<TsavoriteCacheOptions>
    {
        ValidateOptionsResult IValidateOptions<TsavoriteCacheOptions>.Validate(string? name, TsavoriteCacheOptions options)
        {
            // currently nothing to validate
            return ValidateOptionsResult.Success;
        }
    }
}
