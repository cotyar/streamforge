using StreamsForge.Abstractions;

namespace StreamsForge.AppCore;

/// <summary>What resolving one id-or-name reference produced. Three outcomes, because two are a lie:
/// collapsing <see cref="Ambiguous"/> into <see cref="NotFound"/> tells a caller the entity does not
/// exist when in fact two do, and collapsing it into a bad-request tells the caller they typed something
/// wrong when the catalog, not the request, is what is ambiguous.</summary>
public enum EntityRefOutcome
{
    Found,
    NotFound,

    /// <summary>Two or more entities of this kind carry the queried NAME. HTTP <b>409</b> with the
    /// candidates named; gRPC <c>FailedPrecondition</c> — deliberately NOT <c>NotFound</c>, which is the
    /// mapping the shape of this enum exists to prevent someone reaching for.</summary>
    Ambiguous,
}

/// <summary>The outcome of one resolution, carrying everything both transports need so neither
/// re-derives it: the entity on the <see cref="EntityRefOutcome.Found"/> branch, the candidate ids on the
/// <see cref="EntityRefOutcome.Ambiguous"/> branch, and a rendered <see cref="Message"/> for the two
/// failure branches (a caller that formats its own message is a caller that will format it differently
/// from the next one, which is the class of drift this whole type exists to end).</summary>
/// <param name="Outcome">Which branch. Everything else is derived from this.</param>
/// <param name="Value">The resolved entity — non-null iff <see cref="EntityRefOutcome.Found"/>.</param>
/// <param name="Kind">"source" | "pipeline" | "table", for messages.</param>
/// <param name="Query">Exactly what the caller asked for, echoed for messages.</param>
/// <param name="CandidateIds">Ids of the colliding entities, in catalog order — empty unless
/// <see cref="EntityRefOutcome.Ambiguous"/>. Ids, not names: the names are all identical (that is what
/// ambiguity means here), so the id is the only thing a caller can retry with.</param>
public sealed record EntityRefResult<T>(
    EntityRefOutcome Outcome,
    T? Value,
    string Kind,
    string Query,
    IReadOnlyList<string> CandidateIds)
    where T : class
{
    public bool IsFound => Outcome == EntityRefOutcome.Found;

    /// <summary>Empty when <see cref="IsFound"/>; otherwise the sentence a 404 / 409 / gRPC status should
    /// carry verbatim. The ambiguous form names the ids because retrying by id is the caller's only
    /// escape.</summary>
    public string Message => Outcome switch
    {
        EntityRefOutcome.Found => "",
        EntityRefOutcome.NotFound => $"{Kind} '{Query}' not found",
        _ => $"{CandidateIds.Count} {Kind}s are named '{Query}' — address one by id: {string.Join(", ", CandidateIds)}",
    };
}

/// <summary>
/// Plan 016 wave 1 — <b>the one</b> id-or-name resolver. Before this, four call sites hand-rolled it and
/// disagreed: some took the first duplicate silently, some returned "not found" on a duplicate, and none
/// of them could tell a caller which of the two entities they meant.
///
/// <para><b>The rule, and why it is this narrow.</b> Exact ordinal <c>Id</c> wins outright; else exact
/// ordinal <c>Name</c> — 1 match → Found, 0 → NotFound, ≥2 → Ambiguous carrying the candidate ids.
/// Sources are name-only (they have no id, by decision: a source's name is simultaneously its REST route,
/// grain/actor key, stream key, SQL namespace entry, field-number key and every federated peer's entity
/// key). There is <b>no</b> case-insensitive, prefix or fuzzy matching, and that is not laziness: the
/// registry builds the SQL namespace with ORDINAL dictionaries, so a looser resolver here would let
/// <c>GET /api/tables/Trades</c> and <c>FROM Trades</c> resolve to different entities — a divergence
/// between the API surface and the query engine that no error message would ever explain.</para>
///
/// <para><b>Why a pure helper and not a facade member.</b> <c>Facades.cs</c> says twice that existing
/// facade members are frozen because test fakes implement them, and a new facade interface would force
/// two runtimes to implement identical pure code — the duplication <c>SourceKindDispatch</c> exists to
/// eliminate. The logic is pure over lists every call site already fetches.</para>
///
/// <para>ponytail: linear scans over the catalog lists, no index. Ceiling: O(n) per resolve on catalogs
/// of a few hundred entities, which is noise next to the async facade call that fetched the list.
/// Upgrade path: if a call site ever resolves in a loop, build the two ordinal dictionaries once at that
/// site and keep this for the single-shot case.</para>
/// </summary>
public static class EntityRef
{
    public const string SourceKind = "source";
    public const string PipelineKind = "pipeline";
    public const string TableKind = "table";

    /// <summary>Sources are name-only — there is no id branch to try, and that asymmetry is the point:
    /// a source cannot be renamed, so its name IS a stable key.</summary>
    public static EntityRefResult<SourceDefinition> Resolve(IReadOnlyList<SourceDefinition> sources, string query) =>
        Resolve(sources, query, SourceKind, idOf: null, s => s.Name);

    public static EntityRefResult<PipelineDefinition> Resolve(IReadOnlyList<PipelineDefinition> pipelines, string query) =>
        Resolve(pipelines, query, PipelineKind, p => p.Id, p => p.Name);

    public static EntityRefResult<TableDefinition> Resolve(IReadOnlyList<TableDefinition> tables, string query) =>
        Resolve(tables, query, TableKind, t => t.Id, t => t.Name);

    private static EntityRefResult<T> Resolve<T>(
        IReadOnlyList<T> entities, string query, string kind, Func<T, string>? idOf, Func<T, string> nameOf)
        where T : class
    {
        if (string.IsNullOrEmpty(query))
        {
            return new EntityRefResult<T>(EntityRefOutcome.NotFound, null, kind, query ?? "", []);
        }

        if (idOf is not null)
        {
            // "Wins outright": an id match ends resolution even if some OTHER entity's name equals the
            // query. ponytail: first match, not a uniqueness check — ids are registry-minted GUIDs, so
            // two entities sharing one is a corrupted catalog, not a state a caller can produce.
            foreach (var e in entities)
            {
                if (string.Equals(idOf(e), query, StringComparison.Ordinal))
                {
                    return new EntityRefResult<T>(EntityRefOutcome.Found, e, kind, query, []);
                }
            }
        }

        var byName = entities.Where(e => string.Equals(nameOf(e), query, StringComparison.Ordinal)).ToList();
        return byName.Count switch
        {
            1 => new EntityRefResult<T>(EntityRefOutcome.Found, byName[0], kind, query, []),
            0 => new EntityRefResult<T>(EntityRefOutcome.NotFound, null, kind, query, []),
            _ => new EntityRefResult<T>(
                EntityRefOutcome.Ambiguous, null, kind, query,
                // Sources have no id; if they ever collide by name the catalog is already broken, so the
                // name is the only handle there is to report.
                [.. byName.Select(e => idOf is null ? nameOf(e) : idOf(e))]),
        };
    }
}
