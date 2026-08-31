using StreamsForge.Engine.Runtime;

namespace StreamsForge.Engine.Sql;

/// <summary>One scalar SQL function contributed from outside the Engine. Implementations must be
/// <b>total and pure</b>: never throw, never depend on anything but their arguments. Every built-in
/// scalar in this dialect follows that rule — an unconvertible value is NULL, because a per-row
/// exception kills the pipeline for every other row too — and a registered function that throws would
/// break exactly that guarantee.</summary>
public interface IScalarFunction
{
    /// <summary>Case-insensitive; may not collide with a built-in (see <see cref="SqlFunctions"/>).</summary>
    string Name { get; }

    /// <summary>Called at validate time. False produces this dialect's usual "wrong number of
    /// arguments" diagnostic, positioned on the call.</summary>
    bool IsValidArity(int argCount);

    /// <summary>The result kind given each argument's inferred kind, where an entry is null when the
    /// Engine could not infer it (a NULL literal, an unresolved column). Return null to leave the
    /// result kind unknown — the same tolerance COALESCE has — rather than guessing.</summary>
    FieldKind? ResultKind(IReadOnlyList<FieldKind?> argKinds);

    /// <summary>Called per row with already-evaluated arguments (arity already checked). NULL in
    /// should generally mean NULL out. Must not throw.</summary>
    object? Eval(IReadOnlyList<object?> args);
}

/// <summary>One aggregate contributed from outside the Engine. Both accumulators are required because
/// this dialect runs the same SQL two ways: pipelines fold forward over a stream
/// (<see cref="Aggregator"/>), tables maintain a Z-set where a superseded row arrives again with weight
/// −1 (<see cref="IZAggregator"/>). An aggregate that cannot subtract cannot be maintained
/// incrementally, which is why the Z half is not optional.</summary>
public interface IAggregateFunction
{
    string Name { get; }

    /// <summary>Result kind given the argument's inferred kind (null when not inferred). Return null to
    /// leave it unknown.</summary>
    FieldKind? ResultKind(FieldKind? argKind);

    Aggregator CreateStream();

    IZAggregator CreateZ();
}

/// <summary>
/// The registry the Validator, Parser and evaluator consult <b>after</b> their built-in switches, so an
/// assembly outside the Engine can add SQL functions without editing it — the seam
/// <c>StreamsForge.Quant</c> (QuantLib pricing via QLNet) needs, since the Engine must not take a
/// pricing-library dependency to gain a BS_PRICE function.
///
/// <para>Deliberately the same shape as <c>AppCore.Transports.PolledTransports</c>: a static list, not
/// DI discovery, registered from host startup before anything compiles SQL. That class's doc argues the
/// case (the consumers are an Orleans grain and a Dapr actor built by runtime machinery whose container
/// is not the host's); nothing about SQL changes the direction, so it is referenced rather than
/// re-argued. Registration is idempotent by name.</para>
///
/// <para><b>Built-ins always win.</b> A registration whose name collides with a built-in function or
/// aggregate is rejected loudly at registration time rather than silently shadowing it — a third party
/// redefining SUM would change the meaning of SQL already deployed, and the failure would surface as
/// wrong numbers rather than as an error.</para>
/// </summary>
public static class SqlFunctions
{
    private static readonly Lock Gate = new();
    private static readonly List<IScalarFunction> Scalars = [];
    private static readonly List<IAggregateFunction> Aggregates = [];

    /// <summary>Names the Engine implements itself. Kept here rather than read out of the Validator so
    /// the collision check does not depend on that type's internals; <see cref="BuiltInScalarNames"/> is
    /// asserted against the Validator's own set by a test, which is what keeps the two honest.</summary>
    private static readonly string[] BuiltInScalars =
    [
        "ABS", "ROUND", "UPPER", "LOWER", "COALESCE",
        "TO_LONG", "TO_DOUBLE", "TO_BOOL", "TO_TIMESTAMP", "TO_STRING",
        "IF",
    ];

    private static readonly string[] BuiltInAggregates = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

    public static IReadOnlyList<string> BuiltInScalarNames => BuiltInScalars;

    public static IReadOnlyList<string> BuiltInAggregateNames => BuiltInAggregates;

    /// <summary>Registers a scalar function. Re-registering the same name replaces the previous entry
    /// (so a host that runs its registration twice is harmless); colliding with a built-in throws.</summary>
    public static void Register(IScalarFunction function)
    {
        ArgumentNullException.ThrowIfNull(function);
        RejectReservedName(function.Name);
        lock (Gate)
        {
            Scalars.RemoveAll(f => Same(f.Name, function.Name));
            Scalars.Add(function);
        }
    }

    /// <summary>Registers an aggregate. Same replace-by-name and built-in-collision rules as
    /// <see cref="Register(IScalarFunction)"/>.</summary>
    public static void Register(IAggregateFunction aggregate)
    {
        ArgumentNullException.ThrowIfNull(aggregate);
        RejectReservedName(aggregate.Name);
        lock (Gate)
        {
            Aggregates.RemoveAll(a => Same(a.Name, aggregate.Name));
            Aggregates.Add(aggregate);
        }
    }

    public static IScalarFunction? FindScalar(string? name)
    {
        if (string.IsNullOrEmpty(name)) return null;
        lock (Gate)
        {
            return Scalars.FirstOrDefault(f => Same(f.Name, name));
        }
    }

    public static IAggregateFunction? FindAggregate(string? name)
    {
        if (string.IsNullOrEmpty(name)) return null;
        lock (Gate)
        {
            return Aggregates.FirstOrDefault(a => Same(a.Name, name));
        }
    }

    /// <summary>Every registered scalar name, sorted — for the "which functions exist" surfaces (the
    /// console's completion list, `GET /api/sql/functions`, docs).</summary>
    public static IReadOnlyList<string> RegisteredScalarNames()
    {
        lock (Gate)
        {
            return Scalars.Select(f => f.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        }
    }

    public static IReadOnlyList<string> RegisteredAggregateNames()
    {
        lock (Gate)
        {
            return Aggregates.Select(a => a.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        }
    }

    /// <summary>Test seam only — the registry is process-global, so a test that registers something has
    /// to be able to put it back.</summary>
    public static void Clear()
    {
        lock (Gate)
        {
            Scalars.Clear();
            Aggregates.Clear();
        }
    }

    private static void RejectReservedName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("A SQL function name may not be empty.", nameof(name));
        }
        if (BuiltInScalars.Any(b => Same(b, name)) || BuiltInAggregates.Any(b => Same(b, name)))
        {
            throw new ArgumentException(
                $"'{name}' is a built-in function and cannot be replaced — a registration that shadowed it would " +
                "change the meaning of SQL already written against the built-in, and the damage would show up as " +
                "wrong values rather than as an error.", nameof(name));
        }
    }

    private static bool Same(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
}
