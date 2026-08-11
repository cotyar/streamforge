using NATS.Client.Core;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Nats;

/// <summary>
/// Plan 009: the one place that turns this platform's NATS credential fields into a
/// <see cref="NatsOpts"/>. Both directions need it — the <see cref="SourceKinds.Nats"/> subscriber
/// (wave B1) and the sink publisher (wave B2) — and they are otherwise unrelated code, so this exists
/// specifically so the auth precedence rule below is written down once instead of twice.
/// </summary>
public static class NatsConnectionSettings
{
    /// <summary>Precedence: a .creds file beats a token, which beats user+password. Deliberately an
    /// ordered choice rather than "whichever is set", because a config carrying two credentials is a
    /// mistake we should resolve the same way every time instead of by field order.</summary>
    public static NatsOpts Build(
        string url, string? token, string? username, string? password, string? credentials, string name)
    {
        var opts = NatsOpts.Default with
        {
            Url = string.IsNullOrWhiteSpace(url) ? NatsOpts.Default.Url : url,
            Name = name,
        };

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
