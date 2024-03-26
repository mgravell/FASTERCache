using FASTER.core;
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
    public string Directory { get; set; } = "";

    public LogSettings LogSettings { get; } = new();

    public bool DeleteOnClose { get; set; } = false;
    public long LogCapacity { get; set; } = -1;
    public long ObjectLogCapacity { get; set; } = -1;

    internal class Validator : IValidateOptions<FASTERCacheOptions>
    {
        ValidateOptionsResult IValidateOptions<FASTERCacheOptions>.Validate(string? name, FASTERCacheOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.Directory))
            {
                return ValidateOptionsResult.Fail(nameof(options.Directory) + " must be specified");
            }
            if (File.Exists(options.Directory))
            {
                return ValidateOptionsResult.Fail(nameof(options.Directory) + " is a file; must be a directory");
            }
            

            return ValidateOptionsResult.Success;
        }
    }
}
