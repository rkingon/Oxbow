using Microsoft.Extensions.Options;

namespace Oxbow.Internal;

internal sealed class OxbowOptionsValidator : IValidateOptions<OxbowOptions>
{
    public ValidateOptionsResult Validate(string? name, OxbowOptions options)
    {
        var failures = new List<string>();
        try
        {
            Identifiers.Validate(options.Schema, options.TablePrefix);
        }
        catch (ArgumentException ex)
        {
            failures.Add(ex.Message);
        }

        foreach (var (queue, queueOptions) in options.Queues)
        {
            if (string.IsNullOrWhiteSpace(queue) || queue.Length >= 128 || queue == "*")
            {
                failures.Add($"Queue name '{queue}' must be 1 to 127 characters, not blank and not '*'.");
            }

            if (queueOptions.MaxWorkers is < 1 or > 10_000)
            {
                failures.Add($"Queue '{queue}': MaxWorkers must be between 1 and 10,000.");
            }

            if (queueOptions.ConcurrencyLimit is { } limit && (limit.Global < 1 || limit.Local < 1))
            {
                failures.Add($"Queue '{queue}': ConcurrencyLimit.Global and Local must be at least 1, or null for no limit.");
            }
        }

        var periodicNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var job in options.PeriodicJobs)
        {
            if (job.Name is not { } periodicName)
            {
                continue;
            }

            if (!PeriodicJobNames.IsValid(periodicName))
            {
                failures.Add($"Periodic job name '{periodicName}' must be 1 to 255 characters and not blank.");
            }
            else if (!periodicNames.Add(periodicName))
            {
                failures.Add($"Periodic job name '{periodicName}' is registered twice.");
            }
        }

        if (options.MaxAttempts is < 1 or > short.MaxValue)
        {
            failures.Add($"MaxAttempts must be between 1 and {short.MaxValue}.");
        }

        RequirePositive(failures, nameof(options.FetchCooldown), options.FetchCooldown, allowZero: true);
        RequirePositive(failures, nameof(options.FetchPollInterval), options.FetchPollInterval);
        RequirePositive(failures, nameof(options.SchedulerInterval), options.SchedulerInterval);
        RequirePositive(failures, nameof(options.RescueStuckJobsAfter), options.RescueStuckJobsAfter);
        RequirePositiveOrInfinite(failures, nameof(options.JobTimeout), options.JobTimeout);
        RequirePositiveOrInfinite(failures, nameof(options.CompletedJobRetention), options.CompletedJobRetention);
        RequirePositiveOrInfinite(failures, nameof(options.CancelledJobRetention), options.CancelledJobRetention);
        RequirePositiveOrInfinite(failures, nameof(options.DiscardedJobRetention), options.DiscardedJobRetention);
        RequirePositiveOrInfinite(failures, nameof(options.BatchRetention), options.BatchRetention);

        if (options.JsonSerializerOptions is null)
        {
            failures.Add("JsonSerializerOptions must not be null.");
        }

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }

    private static void RequirePositive(List<string> failures, string name, TimeSpan value, bool allowZero = false)
    {
        if (value < TimeSpan.Zero || (!allowZero && value == TimeSpan.Zero))
        {
            failures.Add($"{name} must be positive.");
        }
    }

    private static void RequirePositiveOrInfinite(List<string> failures, string name, TimeSpan value)
    {
        if (value <= TimeSpan.Zero && !OxbowRuntime.IsInfinite(value))
        {
            failures.Add($"{name} must be positive or Timeout.InfiniteTimeSpan.");
        }
    }
}
