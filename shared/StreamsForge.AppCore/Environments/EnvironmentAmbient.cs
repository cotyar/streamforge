namespace StreamsForge.AppCore.Environments;

/// <summary>
/// Plan 021 wave 0, decision D4 — "which catalog is THIS REQUEST talking to", carried in an
/// <see cref="AsyncLocal{T}"/> because there is no per-request context object in this codebase to put it
/// in: endpoints take <c>ClaimsPrincipal</c>/<c>HttpContext</c> as minimal-API parameters directly, and the
/// eight facade interfaces in <c>Facades.cs</c> are declared frozen in their own doc comments because test
/// fakes implement them. Threading an environment parameter explicitly would touch ~66 endpoint handlers
/// and break every fake behind those interfaces, to say the one thing a request already knows.
///
/// <para><b>Ambient state earns its reputation, so the rules are narrow and they are testable.</b></para>
/// <list type="number">
/// <item>It is SET in exactly one place — the environment middleware in <c>StreamsForge.Api</c> — and
/// nowhere else. A second writer is a bug, not a convenience.</item>
/// <item>It is READ only where a runtime key is composed: the facade implementations in each flavour's
/// <c>Facades/</c> folder. Nothing in the Engine, nothing in a grain, nothing in a supervisor.</item>
/// <item><b>Background work must never read it</b> (D5). Supervisors, the lifecycle orchestrator,
/// connector drivers and stream bridges run on timers and subscriptions, outside any request, where this
/// is empty — and empty means <i>default</i>, so a background reader would silently operate on the wrong
/// catalog. Those paths read the environment off the DEFINITION they are acting on instead.</item>
/// </list>
///
/// <para><see cref="Current"/> is <see cref="EnvKeys.Default"/> unless a request set it, which makes every
/// pre-existing call site correct by construction: no header, no ambient, default environment, byte-identical
/// keys.</para>
/// </summary>
public static class EnvironmentAmbient
{
    private static readonly AsyncLocal<string?> Value = new();

    /// <summary>The environment this request selected, or <see cref="EnvKeys.Default"/> outside a request
    /// and in every background context.</summary>
    public static string Current => Value.Value ?? EnvKeys.Default;

    /// <summary>Set by the environment middleware, and by nothing else. The middleware has already
    /// validated that the environment exists — this does no validation of its own, deliberately: a check
    /// here would run on every read of a value that is written once per request.</summary>
    public static void Set(string env) => Value.Value = env;

    /// <summary>Restores the default. The middleware does not need this — an <see cref="AsyncLocal{T}"/>
    /// does not leak out of the request's execution context — but a test that sets the ambient does, and
    /// <c>ClearAsync</c>-style hygiene is cheaper than a test that only fails when the pool reuses a
    /// thread.</summary>
    public static void Clear() => Value.Value = null;

    /// <summary>Runs <paramref name="action"/> with <paramref name="env"/> current and restores whatever
    /// was current before. For the one legitimate non-request writer: a background job that has read an
    /// entity's own <c>Environment</c> off its definition (D5) and wants the facades it calls to agree.</summary>
    public static async Task WithAsync(string env, Func<Task> action)
    {
        var previous = Value.Value;
        Value.Value = env;
        try
        {
            await action().ConfigureAwait(false);
        }
        finally
        {
            Value.Value = previous;
        }
    }
}
