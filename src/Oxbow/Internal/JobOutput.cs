using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Oxbow.Internal;

internal static class JobOutput
{
    /// <summary>The metadata merge that records <paramref name="output"/>. Throws if it's over the size limit.</summary>
    public static string ToMetadataUpdate(object output, JsonSerializerOptions json)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(output, json.GetTypeInfo(output.GetType()));
        if (bytes.Length > JobRow.MaxOutputSize)
        {
            throw new InvalidOperationException(string.Format(
                CultureInfo.InvariantCulture,
                "Job output is {0:N0} bytes of JSON, over the {1:N0}-byte limit (JobRow.MaxOutputSize).",
                bytes.Length,
                JobRow.MaxOutputSize));
        }

        return "{\"output\":" + Encoding.UTF8.GetString(bytes) + "}";
    }
}
