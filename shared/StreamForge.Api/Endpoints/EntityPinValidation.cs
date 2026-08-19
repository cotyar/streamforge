using StreamForge.Abstractions;
using StreamForge.AppCore;

namespace StreamForge.Api;

/// <summary>
/// Plan 016 wave 2-B: the one call site — used by both <c>TablesEndpoints</c> and
/// <c>PipelinesEndpoints</c>, at create AND update — that turns a request's <c>DependsOn</c> into
/// something safe to store.
///
/// <para><b>Why a pipeline-kind pin is refused, not merely ignored.</b> <see cref="EntityPin"/>'s own doc
/// comment states the rule: <c>Kind</c> is "source" | "table" because pipelines are never depended upon —
/// nothing reads a pipeline's output by name. A pin naming a pipeline would sit in the catalog forever,
/// never satisfied by <c>CatalogRevisions.EvaluatePins</c> (which only ever looks in Sources/Tables), so
/// it would either read as permanently stale or — worse — as silently ignored if the evaluator ever
/// special-cased an unknown kind as "fine". 400 at the write is the only answer that cannot be wrong
/// later.</para>
/// </summary>
internal static class EntityPinValidation
{
    /// <summary>Null request payload normalizes to the empty list — the same "no pins declared" shape
    /// <see cref="TableDefinition.DependsOn"/>/<see cref="PipelineDefinition.DependsOn"/> already default
    /// to, so a create that never mentions <c>dependsOn</c> stores exactly what it did before this
    /// wave.</summary>
    public static List<EntityPin> Normalize(List<EntityPin>? pins) => pins ?? [];

    /// <summary>Null when every pin is well-formed; the message for a 400 <see cref="ErrorResponse"/>
    /// otherwise. Checked BEFORE normalization is stored, so a caller that got past this call never has
    /// an unresolvable pin sitting in the catalog.</summary>
    public static string? Validate(List<EntityPin>? pins)
    {
        if (pins is null || pins.Count == 0)
        {
            return null;
        }

        var badKinds = pins
            .Select(p => p.Kind)
            .Where(k => k != EntityRef.SourceKind && k != EntityRef.TableKind)
            .Distinct()
            .ToList();
        if (badKinds.Count > 0)
        {
            return "dependsOn: invalid kind(s) " +
                string.Join(", ", badKinds.Select(k => $"'{k}'")) +
                $" — a pin's kind must be '{EntityRef.SourceKind}' or '{EntityRef.TableKind}'" +
                " (nothing reads a pipeline's output by name)";
        }

        if (pins.Any(p => string.IsNullOrWhiteSpace(p.Name)))
        {
            return "dependsOn: every pin needs a non-empty name";
        }

        return null;
    }
}
