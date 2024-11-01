using Tsavorite.core;
using Microsoft.Extensions.Options;
using System.IO;

namespace FASTERCache;

/// <summary>
/// Allows configuration of FASTER cache stores; these settings can be shared between
/// multiple cache implementations - for example "Output Cache" and "Distributed Cache"
/// might share a FASTER instance.
/// </summary>
public sealed class FASTERCacheOptions
{
    public KVSettings<SpanByte, SpanByte>? Settings { get; set; }

    public bool SlidingExpiration { get; set; } = true;

    internal class Validator : IValidateOptions<FASTERCacheOptions>
    {
        ValidateOptionsResult IValidateOptions<FASTERCacheOptions>.Validate(string? name, FASTERCacheOptions options)
        {
            // currently nothing to validate
            return ValidateOptionsResult.Success;
        }
    }
}
