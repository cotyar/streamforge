using NATS.Client.Core;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Discovery;

namespace StreamsForge.AppCore.Nats;

/// <summary>
/// Plan 009: the one place that turns this platform's NATS credential fields into a
/// <see cref="NatsOpts"/>. Both directions need it — the <see cref="SourceKinds.Nats"/> subscriber
/// (wave B1) and the sink publisher (wave B2) — and they are otherwise unrelated code, so this exists
/// specifically so the auth precedence rule below is written down once instead of twice.
///
/// <para><b>Plan 016 wave 6:</b> <paramref name="url"/> goes through <see cref="NamedEndpoints.Resolve"/>
/// here, at the one place both directions already funnel through for connection setup. Both callers
/// (<c>NatsClientMessageSource.SubscribeAsync</c>, invoked fresh per (re)connect; <see cref="StreamsForge.AppCore.Sinks.NatsSinkClient"/>'s
/// constructor, invoked on every periodic sink client rebuild) construct a NEW <see cref="NatsConnection"/>
/// each time they call this, so resolving here IS resolving at connect time, every connect — no caching
/// survives a reconnect or a rebuild. A literal URL (no leading <c>@</c>) passes through
/// <see cref="NamedEndpoints.Resolve"/> unchanged, including one with an embedded <c>@</c>
/// (<c>nats://user@host:4222</c>) — <see cref="NamedEndpoints.IsReference"/> only recognizes a value that
/// IS ENTIRELY a reference. An unresolvable <c>@name</c> throws here, which both callers let propagate to
/// their own existing status-error path.</para>
/// </summary>
public static class NatsConnectionSettings
{
    /// <summary>Precedence: a .creds file beats a token, which beats user+password. Deliberately an
    /// ordered choice rather than "whichever is set", because a config carrying two credentials is a
    /// mistake we should resolve the same way every time instead of by field order.
    ///
    /// <para><b><paramref name="tls"/> is additive, not a replacement for the <c>tls://</c> URL
    /// scheme.</b> Null (the default) leaves <see cref="NatsOpts.TlsOpts"/> exactly as
    /// <see cref="NatsOpts.Default"/> sets it — a <c>tls://</c> URL still gets system-trust TLS, a
    /// plain <c>nats://</c> URL stays plaintext, byte-identical to before this parameter existed.
    /// Non-null sets only the non-blank paths given and <see cref="NatsTlsOpts.InsecureSkipVerify"/> as
    /// given; <see cref="NatsTlsOpts.Mode"/> is bumped to <see cref="TlsMode.Require"/> only when at
    /// least one of those is actually set, so a blank <see cref="Abstractions.NatsTlsConfig"/> (every
    /// field null/false) behaves exactly like a null one rather than silently forcing TLS on — the
    /// scheme in <paramref name="url"/> keeps deciding in that case.</para></summary>
    public static NatsOpts Build(
        string url, string? token, string? username, string? password, string? credentials, string name,
        NatsTlsConfig? tls = null)
    {
        var resolvedUrl = NamedEndpoints.Resolve(url);
        var opts = NatsOpts.Default with
        {
            Url = string.IsNullOrWhiteSpace(resolvedUrl) ? NatsOpts.Default.Url : resolvedUrl,
            Name = name,
        };

        if (tls is not null)
        {
            var hasCaFile = !string.IsNullOrWhiteSpace(tls.CaFile);
            var hasCertFile = !string.IsNullOrWhiteSpace(tls.CertFile);
            var hasKeyFile = !string.IsNullOrWhiteSpace(tls.KeyFile);
            if (hasCaFile || hasCertFile || hasKeyFile || tls.InsecureSkipVerify)
            {
                var tlsOpts = opts.TlsOpts with { InsecureSkipVerify = tls.InsecureSkipVerify };
                if (hasCaFile)
                {
                    tlsOpts = tlsOpts with { CaFile = tls.CaFile };
                }

                if (hasCertFile)
                {
                    tlsOpts = tlsOpts with { CertFile = tls.CertFile };
                }

                if (hasKeyFile)
                {
                    tlsOpts = tlsOpts with { KeyFile = tls.KeyFile };
                }

                opts = opts with { TlsOpts = tlsOpts with { Mode = TlsMode.Require } };
            }
        }

        if (!string.IsNullOrWhiteSpace(credentials))
        {
            return opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = credentials } };
        }

        if (!string.IsNullOrWhiteSpace(token))
        {
            return opts with { AuthOpts = NatsAuthOpts.Default with { Token = token } };
        }

        if (!string.IsNullOrWhiteSpace(username))
        {
            return opts with
            {
                AuthOpts = NatsAuthOpts.Default with { Username = username, Password = password ?? "" },
            };
        }

        return opts;
    }

    /// <summary>Subject template expansion for sinks: <c>{name}</c> becomes the pipeline or table name,
    /// so one spec can serve a whole catalog. Anything else is passed through untouched — this is
    /// deliberately not a general template language.</summary>
    public static string ExpandSubject(string template, string entityName) =>
        string.IsNullOrWhiteSpace(template) ? entityName : template.Replace("{name}", entityName);
}
