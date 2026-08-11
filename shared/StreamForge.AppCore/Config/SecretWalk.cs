using System.Collections.Concurrent;
using System.Reflection;
using StreamForge.Abstractions;

namespace StreamForge.AppCore.Config;

/// <summary>
/// Plan 010: finds every <see cref="SecretAttribute"/>-marked string property in a config object graph, so
/// <see cref="SecretsMasker"/>'s three operations (mask / restore-from-stored / has-masked) each become one
/// loop over slots instead of one hand-written block per transport per operation.
///
/// <para><b>What it walks.</b> Public readable+writable instance properties. A <c>string</c> property marked
/// <see cref="SecretAttribute"/> is a slot; any other non-null property whose type is a class declared in the
/// <c>StreamForge.Abstractions</c> assembly is recursed into. Everything else — value types, strings without
/// the attribute, collections, framework types — is skipped, which is why <see cref="UrlPollConfig.Headers"/>
/// (a dictionary whose VALUES are secrets) and <c>IngestConfig.Keys</c> (matched by id) are untouched here and
/// keep their existing hand-written handling.</para>
///
/// <para><b>The stored counterpart is walked in lockstep</b> by property name, which is what lets
/// <c>MergeSecrets</c> ("a written *** means keep the stored value") work generically.
/// <see cref="SecretSlot.HasStored"/> distinguishes "there is no corresponding stored object" from "the stored
/// object's value is null" — the pre-plan-010 code assigned the stored value whenever the stored NODE existed,
/// even when that value was null, and that behavior is preserved deliberately.</para>
/// </summary>
public static class SecretWalk
{
    /// <summary>Depth cap. Config graphs are shallow trees (2–3 levels); this only exists so a future
    /// self-referencing contract cannot turn a masking call into a stack overflow.</summary>
    private const int MaxDepth = 8;

    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertyCache = new();

    /// <summary>One secret-valued property found on a live object: its current value, its counterpart in the
    /// "stored" graph (if that graph had a corresponding object), and a setter bound to the instance.</summary>
    public readonly record struct SecretSlot(string? Value, string? StoredValue, bool HasStored, Action<string?> Set);

    /// <summary>Enumerates every secret slot reachable from <paramref name="root"/>. <paramref name="stored"/>
    /// may be null (nothing to compare against — every slot reports <c>HasStored: false</c>) or a graph of the
    /// same shape, in which case each slot also carries the stored value at the same property path.</summary>
    public static IEnumerable<SecretSlot> Slots(object? root, object? stored = null) => Walk(root, stored, 0);

    private static IEnumerable<SecretSlot> Walk(object? node, object? stored, int depth)
    {
        if (node is null || depth > MaxDepth)
        {
            yield break;
        }

        foreach (var prop in PropertiesOf(node.GetType()))
        {
            var value = prop.GetValue(node);

            if (prop.PropertyType == typeof(string))
            {
                if (prop.IsDefined(typeof(SecretAttribute), inherit: true))
                {
                    var (storedValue, hasStored) = StoredStringAt(stored, prop.Name);
                    var target = node;
                    yield return new SecretSlot(
                        (string?)value, storedValue, hasStored, v => prop.SetValue(target, v));
                }

                continue;
            }

            if (value is null || !IsContractClass(prop.PropertyType))
            {
                continue;
            }

            var storedChild = StoredObjectAt(stored, prop.Name);
            foreach (var slot in Walk(value, storedChild, depth + 1))
            {
                yield return slot;
            }
        }
    }

    /// <summary>Only classes from the contracts assembly are recursed into — that boundary is what keeps the
    /// walk inside our own data shapes instead of wandering into framework object graphs.</summary>
    private static bool IsContractClass(Type type) =>
        type.IsClass && type != typeof(string) && type.Assembly == typeof(SecretAttribute).Assembly;

    private static (string? Value, bool HasStored) StoredStringAt(object? stored, string name)
    {
        if (stored is null)
        {
            return (null, false);
        }

        var prop = PropertiesOf(stored.GetType()).FirstOrDefault(p => p.Name == name && p.PropertyType == typeof(string));
        return prop is null ? (null, false) : ((string?)prop.GetValue(stored), true);
    }

    private static object? StoredObjectAt(object? stored, string name) =>
        stored is null ? null : PropertiesOf(stored.GetType()).FirstOrDefault(p => p.Name == name)?.GetValue(stored);

    private static PropertyInfo[] PropertiesOf(Type type) =>
        PropertyCache.GetOrAdd(type, static t =>
            [.. t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanRead && p.CanWrite)]);
}
