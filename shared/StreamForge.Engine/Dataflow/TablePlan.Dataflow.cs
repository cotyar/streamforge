namespace StreamForge.Engine;

/// <summary>Plan 003 M2 additive member on the frozen <see cref="TablePlan"/> contract (see PublicApi.cs's
/// header comment — that file itself is untouched; this is a new partial-class file, additive-only, per
/// the M2 task's "PublicApi.cs: additive members only" constraint). Builds the compiled plan's partitioned
/// dataflow graph — see <see cref="Dataflow.TableDataflowPlan"/>.</summary>
public sealed partial class TablePlan
{
    public Dataflow.TableDataflowPlan CreateDataflow(int partitionCount) => new(Compiled, partitionCount);
}
