using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StreamForge.Abstractions;

namespace StreamForge.Api.Auth;

// =================================================================================================
// Plan 015 wave 4-D — the sweeper that makes escalation happen, and the chat's approval filer.
//
// Escalation and expiry are driven by a shared hosted BackgroundService, not by grain timers or Dapr
// reminders: the Dapr compose stack runs with NO scheduler, so reminders are off the table and one
// shape has to work identically on both flavours (015 D:"Escalation is driven by a shared hosted
// sweeper"). Both hosts already run BackgroundServices, and both call AddStreamForgeApi — so this is
// registered there, and neither Program.cs is touched.
// =================================================================================================

/// <summary>The <c>Approvals:*</c> configuration, read once in <c>AddStreamForgeApi</c> and passed
/// around as a value so the sweeper and the chat filer cannot disagree about whether the feature is
/// on.</summary>
/// <param name="Enabled"><c>Approvals:Enabled</c>, <b>default false</b>. Approvals ship inert so that
/// existing deployments are byte-identical and both suites stay green without touching a pre-existing
/// test. Off means: the sweeper does not run, and the chat files nothing.</param>
/// <param name="SweepSeconds"><c>Approvals:SweepSeconds</c>, default 30. Clamped to at least 1 — a
/// zero or negative period is a spin loop, and there is no reading of "sweep every 0 seconds" that
/// anybody wants.</param>
public sealed record ApprovalOptions(bool Enabled, int SweepSeconds)
{
    public const string EnabledKey = "Approvals:Enabled";
    public const string SweepSecondsKey = "Approvals:SweepSeconds";
    public const int DefaultSweepSeconds = 30;

    public TimeSpan SweepInterval => TimeSpan.FromSeconds(Math.Max(1, SweepSeconds));
}

/// <summary>
/// Expiry and escalation, once per <c>Approvals:SweepSeconds</c>.
///
/// <para><b>It does not run at all when approvals are disabled</b> — no timer, no facade resolution, no
/// allocation past the service object itself. "Inert" has to mean inert, or the byte-identical claim is
/// only a claim.</para>
///
/// <para><b>It must never be the thing that crashes a host.</b> A sibling wave is implementing
/// <see cref="IApprovalFacade.SweepAsync"/> on both flavours right now, so against some trees the
/// facade is unregistered, unimplemented, or throwing. All three are the same case here: log, and come
/// back at the next tick. It cannot spin, because the tick is what paces it — a failing sweep waits the
/// full interval exactly like a successful one, so a store that is down costs one log line per interval
/// and nothing else.</para>
/// </summary>
public sealed class ApprovalSweeperService(
    Func<IApprovalFacade?> facade,
    ApprovalOptions options,
    ILogger<ApprovalSweeperService> logger) : BackgroundService
{
    private long _sweeps;
    private long _failures;

    /// <summary>Completed sweeps (a sweep that threw does not count). Diagnostics and tests.</summary>
    public long Sweeps => Interlocked.Read(ref _sweeps);

    /// <summary>Sweeps that threw or found no store. Diagnostics and tests.</summary>
    public long Failures => Interlocked.Read(ref _failures);

    /// <summary>How many requests the last completed sweep changed state on; -1 before the first one.</summary>
    public int LastChanged { get; private set; } = -1;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // No Task.Yield(): the first await below is PeriodicTimer.WaitForNextTickAsync, which is
        // inherently asynchronous, so the host is released without one and everything above it — the
        // disabled check especially — happens deterministically.
        if (!options.Enabled)
        {
            logger.LogDebug("{Key}=false — the approval sweeper is not running.", ApprovalOptions.EnabledKey);
            return;
        }

        logger.LogInformation(
            "Approval sweeper running every {Seconds}s ({Key}).",
            options.SweepInterval.TotalSeconds,
            ApprovalOptions.SweepSecondsKey);

        using var timer = new PeriodicTimer(options.SweepInterval);

        // The first sweep waits one interval on purpose: at t=0 the store may still be joining (Orleans)
        // or the sidecar may not be answering (Dapr), and nothing can have expired in the first thirty
        // seconds of a process's life that could not wait another thirty.
        while (await SafeWaitAsync(timer, stoppingToken).ConfigureAwait(false))
        {
            await SweepOnceAsync().ConfigureAwait(false);
        }
    }

    /// <summary>One sweep, with every failure absorbed. Exposed so a test can drive the sweep without
    /// waiting out a timer — the loop above adds only the pacing.</summary>
    public async Task SweepOnceAsync()
    {
        try
        {
            var store = facade();
            if (store is null)
            {
                Interlocked.Increment(ref _failures);
                LogTrouble(null, "no IApprovalFacade is registered on this host");
                return;
            }

            LastChanged = await store.SweepAsync(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).ConfigureAwait(false);
            Interlocked.Increment(ref _sweeps);

            if (LastChanged > 0)
            {
                logger.LogInformation("Approval sweep expired or escalated {Count} request(s).", LastChanged);
            }
        }
        catch (Exception ex)
        {
            // Everything: a NotImplementedException from a store the sibling wave has not finished, a
            // grain call timeout, a sidecar 500. None of them is worth taking a host down for, and none
            // of them is worth a stack trace every thirty seconds forever.
            Interlocked.Increment(ref _failures);
            LogTrouble(ex, "the approval store failed");
        }
    }

    private void LogTrouble(Exception? ex, string what)
    {
        var failures = Interlocked.Read(ref _failures);

        // ponytail: first, then every tenth. Ceiling: a store that is down for hours produces one line
        // per five minutes at the default interval instead of one per thirty seconds. Upgrade path is a
        // real backoff on the day the interval becomes configurable down to seconds.
        if (failures == 1 || failures % 10 == 0)
        {
            logger.LogWarning(ex, "Approval sweep #{Failures} did not run: {What}. Retrying at the next interval.", failures, what);
        }
    }

    private static async Task<bool> SafeWaitAsync(PeriodicTimer timer, CancellationToken token)
    {
        try
        {
            return await timer.WaitForNextTickAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}

/// <summary>
/// Wave 3-C's <c>IChatApprovalFiler</c> seam, filled: a <see cref="AccessDecision.RequiresApproval"/>
/// answer to a chat tool becomes a real <see cref="ApprovalRequest"/> in the store, and the model is
/// handed the id the store assigned.
///
/// <para><b>When approvals are disabled it keeps wave 3-C's honest behaviour and returns null.</b>
/// Filing into a feature nobody turned on would produce a request that no sweeper expires, no inbox
/// shows and no approver can act on — an id that looks like a promise and is not one. Wave 3-C's
/// reasoning is the same reasoning: inventing an approval id sends a user hunting for a request that
/// does not exist. So the gate falls back to the correlation id, labelled as a correlation id, and the
/// model tells the user to go and find an administrator.</para>
///
/// <para>A store that throws is the same case: null, one log line, nothing executed. The tool was going
/// to be refused either way — the only question is which sentence the model gets, and "no request was
/// filed" is the true one.</para>
/// </summary>
public sealed class ApprovalStoreChatFiler(
    Func<IApprovalFacade?> facade,
    ApprovalOptions options,
    ILogger<ApprovalStoreChatFiler> logger) : IChatApprovalFiler
{
    public async Task<string?> FileAsync(ApprovalRequest draft, CancellationToken ct)
    {
        if (!options.Enabled)
        {
            logger.LogWarning(
                "Chat tool needs approval for {Action} on {Scope} (requested by {RequestedBy}) but {Key}=false — nothing filed, nothing executed. Correlation {Correlation}.",
                draft.Action,
                draft.Scope,
                draft.RequestedBy,
                ApprovalOptions.EnabledKey,
                draft.Id);
            return null;
        }

        try
        {
            var store = facade();
            if (store is null)
            {
                logger.LogWarning(
                    "Chat tool needs approval for {Action} on {Scope} but no IApprovalFacade is registered — nothing filed. Correlation {Correlation}.",
                    draft.Action,
                    draft.Scope,
                    draft.Id);
                return null;
            }

            // The store stamps Id/RequestedAtMs/ExpiresAtMs/State (IApprovalFacade.RequestAsync), so the
            // id that comes back is the one an approver will see — not the correlation id the draft
            // carried in.
            var stored = await store.RequestAsync(draft).ConfigureAwait(false);
            var id = string.IsNullOrEmpty(stored?.Id) ? null : stored!.Id;

            if (id is null)
            {
                logger.LogWarning(
                    "The approval store accepted {Action} on {Scope} but returned no id — treating it as unfiled. Correlation {Correlation}.",
                    draft.Action,
                    draft.Scope,
                    draft.Id);
                return null;
            }

            logger.LogInformation(
                "Filed approval {ApprovalId} for {Action} on {Scope}, requested by {RequestedBy} via the AI chat.",
                id,
                draft.Action,
                draft.Scope,
                draft.RequestedBy);
            return id;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Filing an approval for {Action} on {Scope} failed — nothing filed, nothing executed. Correlation {Correlation}.",
                draft.Action,
                draft.Scope,
                draft.Id);
            return null;
        }
    }
}
