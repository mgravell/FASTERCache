using FASTER.core;
using Microsoft.Extensions.Options;
using System.IO;

namespace FASTERCache;
public sealed class FASTERCacheOptions
{
    public string Directory { get; set; } = "";

    public LogSettings LogSettings { get; } = new();

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
