using System.Security.Claims;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using StreamsForge.Abstractions;
using StreamsForge.AppCore.Config;

namespace StreamsForge.Api.Auth;

/// <summary>
/// Plan 015 wave 5-B — <see cref="AuditEntry.BeforeJson"/> / <see cref="AuditEntry.AfterJson"/>, filled
/// at the nine catalog mutation sites (create / update / delete × source / pipeline / table).
///
/// <para>Before this, an audit row said "alice updated pipeline prod-orders" and stopped there — which
/// is the row you have, and never the answer to the first question anyone asks after an incident. The
/// two fields existed on the contract from wave 0 and nothing filled them.</para>
///
/// <h3>1. Every value is masked, and the masking is the export path's, not a list of field names</h3>
///
/// <para><b>A source definition contains credentials.</b> Serialising one into an audit row without
/// plan 010's pass would turn the audit log — append-only, readable by anyone holding
/// <see cref="Actions.AuditRead"/>, retained at up to <c>Audit:MaxEntriesPerDay</c> rows a day, and
/// thought of by nobody as a secret store — into a plaintext credential dump with a history feature.
/// That is strictly worse than the bug <c>[Secret]</c> was introduced to prevent: the config-export
/// path at least has an <c>includeSecrets</c> flag and an Admin check in front of it, and this has
/// neither.</para>
///
/// <para>So a definition reaches a row through exactly one door: <see cref="SecretsMasker"/>'s
/// <see cref="SecretsMasker.Mask(SourceDefinition)"/> / <see cref="SecretsMasker.MaskPipeline"/> /
/// <see cref="SecretsMasker.MaskTable"/> — the same three calls every REST read path already makes.
/// They are descriptor-driven (<see cref="SecretWalk"/> finds every <c>[Secret]</c>-marked property by
/// reflection, plus the two hand-written collection shapes: <c>Url.Headers</c> values and
/// <c>Ingest.Keys[].Hash/.Salt</c>), so an out-of-tree connector that declares a new credential is
/// covered here on the day it is declared, with no edit to this file. The typed overloads below are the
/// enforcement: there is no entry point that takes a definition and does not mask it.</para>
///
/// <h3>2. An update records a DIFF, a create/delete records the whole document</h3>
///
/// <para>A table definition with a large SQL body and a long field list, serialised twice on every
/// update, at up to 20 000 rows a day, is a day shard nobody can load. Three options were on the
/// table — full documents, changed fields only, a cap with a marker — and the answer is not the
/// smallest one but the one that is smallest <i>and</i> the most useful to read afterwards:</para>
/// <list type="bullet">
///   <item><b>Update → the changed top-level properties only, on both sides.</b> "She changed
///   <c>sql</c> and <c>parallelism</c>, here is each before and after" IS the question being asked; two
///   near-identical 8 KB blobs to eyeball is the same information with the answer hidden in it. It also
///   removes the cost where it actually bites: an unchanged SQL body costs nothing on every one of the
///   updates that did not touch it, which is nearly all of them.</item>
///   <item><b>Create → <see cref="AuditEntry.BeforeJson"/> is null and the whole (masked) document is
///   the after; delete → the whole document is the before and after is null.</b> There is nothing to
///   diff against, and on a delete the full document is the only surviving copy of what was there.</item>
///   <item><b>Both sides are capped at <see cref="MaxJsonChars"/>, and the cap degrades to the field
///   NAMES rather than to a truncated blob</b> — see <see cref="Render"/>. A mid-JSON cut produces
///   unparseable text and answers nothing; the names still answer "what did she change".</item>
/// </list>
///
/// <para>ponytail: <see cref="MaxJsonChars"/> is a const, not configuration. Ceiling: a deployment
/// whose real churn is 20 000 mutations a day still stores up to ~8 KB of change detail per row and can
/// only turn the whole thing off with <c>Audit:RecordAllowedMutations=false</c>, which also loses the
/// rows. Upgrade path is an <c>Audit:MaxChangeChars</c> key read in <c>AddStreamsForgeApi</c> and carried
/// on the sink — one key and one field, on the day a deployment's numbers say it matters.</para>
///
/// <h3>3. The diff is decided on the unmasked values and emitted from the masked ones</h3>
///
/// <para>Not a subtlety worth discovering later: masking replaces a rotated password with <c>***</c> on
/// BOTH sides, so a diff computed on masked documents reports a credential rotation as "nothing
/// changed". An audit row that says nothing changed when something did is worse than no row. So
/// <see cref="ChangedKeys"/> compares the <i>unmasked</i> serialisations to decide WHICH top-level
/// properties moved, and every byte that is written into the entry comes from the <i>masked</i> ones —
/// a rotation therefore shows up as <c>connector</c> changed, with <c>***</c> either side, which is
/// exactly the honest report. <b>The unmasked nodes never leave <see cref="ChangedKeys"/>; it returns a
/// set of property names and nothing else.</b></para>
///
/// <h3>4. Attribution comes in on the row, so the chat's does not become "rest"</h3>
///
/// <para>Every method here takes an already-built <see cref="AuditEntry"/> rather than inventing
/// <see cref="AuditEntry.Actor"/> itself. <see cref="RestRow"/> builds the REST one; a caller holding a
/// <c>ChatAttribution</c> passes <c>attribution.Row(action, scope, "executed")</c> instead and its
/// Actor (the model), OnBehalfOf (the human) and Origin (<c>chat</c>) survive untouched — this class
/// writes Before/After/Detail and nothing else onto the row it was given. Today the chat's tools call
/// <c>ICatalogFacade</c> directly and never reach these handlers, so nothing is attributed to the model
/// through here yet; the point is that when a caller does arrive with an attribution, there is no line
/// in this file that can quietly overwrite it with <c>"rest"</c>.</para>
///
/// <h3>5. It cannot make a request fail or slow</h3>
///
/// <para><see cref="IAuditSink.Record"/> is a non-blocking <c>TryWrite</c> and there is no <c>await</c>
/// anywhere below, so a call site's latency and status code are unchanged. Resolving the sink is inside
/// the try as well: on a host (or a test) where <see cref="IAuditSink"/>'s factory throws, the mutation
/// still succeeds and the row is simply not written — audit is never the reason a request fails.</para>
/// </summary>
public static class CatalogChangeAudit
{
    /// <summary>Per-side cap on the serialised change detail. ~4 KB is the same order as a row's
    /// existing <see cref="AuditEntry.Detail"/> and comfortably holds a real diff; past it the row
    /// keeps the changed field NAMES and drops the values (<see cref="Render"/>).</summary>
    public const int MaxJsonChars = 4096;

    /// <summary>From <see cref="AuditEntry.Outcome"/>'s own documented vocabulary. Deliberately NOT
    /// <c>"allowed"</c>: <see cref="AccessGuard"/> already writes an <c>allowed</c> row when the
    /// decision was made, which is a different fact from the mutation having actually happened — a
    /// request can be allowed and then 400 on validation or 404 on a missing id. These rows are written
    /// only after the store said yes, which is what makes <c>executed</c> the true word and what lets a
    /// reader tell the two rows apart.</summary>
    public const string ExecutedOutcome = "executed";

    /// <summary>Only ever used to WRITE. camelCase to match every other JSON the platform emits, enums
    /// as their member names so a stored <c>persistence: "MemoryOnly"</c> reads as itself rather than
    /// as <c>0</c>, and nulls dropped because an audit diff of "this field is still null" is noise.</summary>
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };

    /// <summary>The row a REST mutation is attributed to: the authenticated human, origin <c>rest</c>,
    /// no <see cref="AuditEntry.OnBehalfOf"/> — nobody is acting on anyone's behalf on this path, and a
    /// self-referential value there would make the field useless for the one case it exists for.</summary>
    public static AuditEntry RestRow(ClaimsPrincipal principal, string action, string scope) => new()
    {
        Id = Guid.NewGuid().ToString("n"),
        AtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Actor = principal.Identity?.Name ?? "(anonymous)",
        Action = action,
        Scope = scope,
        Outcome = ExecutedOutcome,
        Origin = "rest",
    };

    /// <summary>A detached copy of a definition, for the two PUT handlers that update the STORED object
    /// in place (<c>existing.Name = req.Name; …</c>) and would otherwise hand this class the same
    /// object twice and report every update as a no-op.
    ///
    /// <para>The same JSON round trip <c>ConfigJsonMapper.DeepCloneModel</c> performs — which is
    /// internal to AppCore, and copying two lines is cheaper than widening that surface for one caller.
    /// Taken at the top of the handler, before the first field assignment; it costs one serialise per
    /// update on a path that is already doing a store write.</para></summary>
    public static T Snapshot<T>(T value) =>
        JsonSerializer.SerializeToNode(value, JsonOptions)!.Deserialize<T>(JsonOptions)!;

    // ---------------------------------------------------------------------------------------------
    // The call sites' entry points. Two overloads per entity type: the REST convenience one that
    // pulls the sink off the request and builds its own row, and the explicit one that takes both — the
    // seam any non-REST caller (the chat, an approval replay) uses to keep its own attribution.
    // ---------------------------------------------------------------------------------------------

    public static void RecordSource(
        HttpContext? http, ClaimsPrincipal principal, string action, string scope,
        SourceDefinition? before, SourceDefinition? after) =>
        RecordSource(SinkOf(http), RestRow(principal, action, scope), before, after);

    public static void RecordSource(IAuditSink? sink, AuditEntry row, SourceDefinition? before, SourceDefinition? after) =>
        Record(sink, row, before, after, static d => SecretsMasker.Mask(d));

    public static void RecordPipeline(
        HttpContext? http, ClaimsPrincipal principal, string action, string scope,
        PipelineDefinition? before, PipelineDefinition? after) =>
        RecordPipeline(SinkOf(http), RestRow(principal, action, scope), before, after);

    public static void RecordPipeline(IAuditSink? sink, AuditEntry row, PipelineDefinition? before, PipelineDefinition? after) =>
        Record(sink, row, before, after, static d => SecretsMasker.MaskPipeline(d));

    public static void RecordTable(
        HttpContext? http, ClaimsPrincipal principal, string action, string scope,
        TableDefinition? before, TableDefinition? after) =>
        RecordTable(SinkOf(http), RestRow(principal, action, scope), before, after);

    public static void RecordTable(IAuditSink? sink, AuditEntry row, TableDefinition? before, TableDefinition? after) =>
        Record(sink, row, before, after, static d => SecretsMasker.MaskTable(d));

    public static void RecordUser(
        HttpContext? http, ClaimsPrincipal principal, string action, string scope,
        UserRecord? before, UserRecord? after) =>
        RecordUser(SinkOf(http), RestRow(principal, action, scope), before, after);

    /// <summary>The credential record's diff — added in wave 6 after the audit console's first live run
    /// showed <c>/api/users</c> writing NO row at all, for either the guard decision or the mutation.
    /// Creating an account and changing someone's role are the most privileged mutations the platform
    /// has, and they were the only ones invisible in the log.
    ///
    /// <para><b>The redactor is a projection, not a mask pass</b>, because <see cref="UserRecord"/> is
    /// the one entity here whose secret is not a config field but the record's whole reason to exist.
    /// <see cref="UserRecord.PasswordHash"/> and <see cref="UserRecord.PasswordSalt"/> are replaced with
    /// the literal mask when non-empty rather than dropped, and that is deliberate: the diff is computed
    /// on the UNMASKED pair (see <see cref="Record{T}"/>), so a password reset moves those keys and the
    /// row therefore reports <c>passwordHash: "***" → "***"</c>. The key's presence is the signal, its
    /// value never is — which is exactly the shape a rotated source credential already has, and the
    /// reason an administrator can see "somebody reset bob's password" without the log holding anything
    /// worth stealing. Dropping the fields instead would have made a password reset render as an empty
    /// diff, i.e. as nothing having happened.</para></summary>
    public static void RecordUser(IAuditSink? sink, AuditEntry row, UserRecord? before, UserRecord? after) =>
        Record(sink, row, before, after, static u => new UserRecord
        {
            Username = u.Username,
            DisplayName = u.DisplayName,
            Role = u.Role,
            CreatedAtMs = u.CreatedAtMs,
            ExternalSubject = u.ExternalSubject,
            IdentityProvider = u.IdentityProvider,
            PasswordHash = u.PasswordHash.Length == 0 ? "" : SourceKinds.SecretMask,
            PasswordSalt = u.PasswordSalt.Length == 0 ? "" : SourceKinds.SecretMask,
        });

    // ---------------------------------------------------------------------------------------------

    /// <param name="redact">The entity type's masking pass. There is no overload that omits it, which
    /// is the only reason this class can claim a plaintext credential cannot reach a row.</param>
    private static void Record<T>(IAuditSink? sink, AuditEntry row, T? before, T? after, Func<T, object> redact)
        where T : class
    {
        if (sink is null || (before is null && after is null))
        {
            return;
        }

        try
        {
            var beforeJson = before is null ? null : NodeOf(redact(before));
            var afterJson = after is null ? null : NodeOf(redact(after));

            // Both sides present = an update, so keep only what moved. Create and delete keep the whole
            // document: there is no other side to compare with, and on a delete this is the last copy.
            if (beforeJson is not null && afterJson is not null)
            {
                var changed = ChangedKeys(before, after);
                Retain(beforeJson, changed);
                Retain(afterJson, changed);
            }

            row.BeforeJson = Render(beforeJson);
            row.AfterJson = Render(afterJson);
            row.Detail ??= before is null ? "created" : after is null ? "deleted" : "updated";
            if (string.IsNullOrEmpty(row.Outcome))
            {
                row.Outcome = ExecutedOutcome;
            }

            sink.Record(row);
        }
        catch (Exception)
        {
            // Swallowed for the same reason AccessGuard.Audit swallows, and with the same absence of a
            // log call: the mutation already happened and succeeded, and nothing about recording it is
            // allowed to change what the caller sees. A sink that throws owns its own reporting.
        }
    }

    /// <summary>
    /// Which top-level properties differ, computed on the <b>unmasked</b> pair.
    ///
    /// <para>This is the one place unmasked values are serialised, and the nodes are local: what leaves
    /// this method is a set of property names. It exists because masking collapses a rotated credential
    /// to <c>***</c> on both sides, and a diff taken after that would report a password change as no
    /// change at all — the single most audit-relevant edit anyone makes to a source, silently missing.
    /// Deciding on the real values and emitting the masked ones reports it as <c>connector</c> changed
    /// with <c>***</c> either side, which is both true and safe.</para>
    /// </summary>
    private static HashSet<string> ChangedKeys(object? before, object? after)
    {
        var a = NodeOf(before);
        var b = NodeOf(after);
        var keys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var key in (a?.Select(p => p.Key) ?? []).Concat(b?.Select(p => p.Key) ?? []))
        {
            var left = a is not null && a.TryGetPropertyValue(key, out var l) ? l : null;
            var right = b is not null && b.TryGetPropertyValue(key, out var r) ? r : null;
            if (!JsonNode.DeepEquals(left, right))
            {
                keys.Add(key);
            }
        }

        return keys;
    }

    /// <summary>Serialised against the RUNTIME type: the masking passes hand back <c>object</c>, and
    /// <c>SerializeToNode</c> given a static <c>object</c> would write <c>{}</c>.</summary>
    private static JsonObject? NodeOf(object? value) =>
        value is null ? null : JsonSerializer.SerializeToNode(value, value.GetType(), JsonOptions) as JsonObject;

    private static void Retain(JsonObject node, HashSet<string> keys)
    {
        foreach (var key in node.Select(p => p.Key).Where(k => !keys.Contains(k)).ToList())
        {
            node.Remove(key);
        }
    }

    /// <summary>Serialises one side, or degrades it to its field names.
    ///
    /// <para>A blob cut off at <see cref="MaxJsonChars"/> is unparseable text that answers nothing; the
    /// names answer "what changed" even when the values had to go, so that is what the marker keeps.
    /// <c>_truncated</c> makes the gap explicit for the same reason <see cref="AuditPage.Truncated"/>
    /// exists one layer down: silence must never read as absence.</para></summary>
    private static string? Render(JsonObject? node)
    {
        if (node is null)
        {
            return null;
        }

        var json = node.ToJsonString(JsonOptions);
        if (json.Length <= MaxJsonChars)
        {
            return json;
        }

        var fields = new JsonArray();
        foreach (var property in node)
        {
            fields.Add((JsonNode)property.Key);
        }

        return new JsonObject
        {
            ["_truncated"] = true,
            ["_chars"] = json.Length,
            ["_fields"] = fields,
        }.ToJsonString(JsonOptions);
    }

    /// <summary>The sink off the request, or null. Wrapped because resolving a service must not be the
    /// thing that fails a mutation which has already been persisted — on a host with no audit sink
    /// registered, or one whose factory throws, the correct outcome is "no row", not a 500.</summary>
    private static IAuditSink? SinkOf(HttpContext? http)
    {
        try
        {
            return http?.RequestServices.GetService<IAuditSink>();
        }
        catch (Exception)
        {
            return null;
        }
    }
}
