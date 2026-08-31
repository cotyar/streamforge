namespace StreamsForge.Host.Services;

/// <summary>Lets hosted services wait until the app (and co-hosted silo) has fully started.</summary>
internal static class StartupSignal
{
    public static async Task WaitForApplicationStartedAsync(IHostApplicationLifetime lifetime, CancellationToken ct)
    {
        if (lifetime.ApplicationStarted.IsCancellationRequested)
        {
            return;
        }

        var tcs = new TaskCompletionSource();
        await using var registration = lifetime.ApplicationStarted.Register(() => tcs.TrySetResult());
        await tcs.Task.WaitAsync(ct);
    }
}
