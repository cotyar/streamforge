using StreamForge.Abstractions;
using StreamForge.AppCore.Transports;

namespace StreamForge.AppCore.Config;

/// <summary>
/// Plan 006 (D-H): secrets-lite masking. Secrets are stored plaintext at rest (no KMS, no encryption —
/// documented ceiling) and masked as <see cref="SourceKinds.SecretMask"/> ("***") on every read
/// path (GET/list/export); a written "***" value means "keep the stored value" — see
/// <see cref="MergeSecrets"/>, which implements that half of the contract for the PUT-replaces-
/// whole-object GET→edit→PUT cycle.
///
/// <para><b>Plan 010: which values are secret is declared on the contracts, not listed here.</b> Every
/// per-transport credential carries <see cref="SecretAttribute"/> and is found by <see cref="SecretWalk"/>,
/// so adding a transport adds ZERO lines to this file. Before that change the same slot had to be named in
/// three places per direction (mask / merge / has-masked), and a slot missed in any one of them leaked a
/// plaintext credential through an export — a silent failure nobody would notice until it mattered.</para>
///
/// <para>Two secret shapes remain hand-written because they are collections with their own matching rules
/// rather than plain properties, and neither multiplies with transports: <see cref="UrlPollConfig.Headers"/>
/// (dictionary VALUES, matched by key) and <c>IngestConfig.Keys[].Hash/.Salt</c> (matched by
/// <c>IngestKey.Id</c>).</para>
/// </summary>
public static class SecretsMasker
{
    /// <summary>Deep clones <paramref name="def"/> and replaces every NON-EMPTY secret value
    /// (header values under Connector.Url.Headers; Connector.Grpc.Password/.Token; plan 009 A1.2:
    /// Ingest.Keys[].Hash/.Salt) with <see cref="SourceKinds.SecretMask"/>. An empty/null secret
    /// value is left as-is (masking an absent secret would fabricate one) — never mutates
    /// <paramref name="def"/>.</summary>
    public static SourceDefinition Mask(SourceDefinition def)
    {
        var clone = ConfigJsonMapper.DeepCloneModel(def);
        MaskIngestKeys(clone.Ingest);

        var connector = clone.Connector;
        if (connector is null)
        {
            return clone;
        }

        if (connector.Url is { } url)
        {
            foreach (var key in url.Headers.Keys.ToList())
            {
                if (!string.IsNullOrEmpty(url.Headers[key]))
                {
                    url.Headers[key] = SourceKinds.SecretMask;
                }
            }
        }

        // Plan 010: every per-transport credential (grpc Password/Token, nats Token/Password/Credentials,
        // and whatever a future transport declares) is found by its [Secret] attribute rather than named
        // here — see SecretWalk. Url.Headers stays above because its secrets are dictionary VALUES.
        foreach (var slot in SecretWalk.Slots(connector))
        {
            if (!string.IsNullOrEmpty(slot.Value))
            {
                slot.Set(SourceKinds.SecretMask);
            }
        }

        MaskSettings(connector.Settings, TransportDescriptors.ForSource(clone.Kind));

        return clone;
    }

    /// <summary>Masks an out-of-tree kind's <c>Settings</c> bag (see
    /// <see cref="ConnectorConfig.Settings"/>). The bag is a plain string dictionary, so
    /// <see cref="SecretWalk"/>'s <c>[Secret]</c> attributes cannot reach into it — which keys are
    /// credentials is read off the kind's own descriptor instead.
    ///
    /// <para><b>An unregistered kind masks EVERYTHING.</b> No descriptor means nothing here can tell a
    /// hostname from a password, and the two failure modes are not symmetric: over-masking makes an
    /// export of a kind whose plugin isn't installed unhelpful, under-masking exports that plugin's
    /// credentials in plaintext. This is the same instinct <c>[Secret]</c> exists to serve, applied
    /// where the attribute cannot go.</para></summary>
    private static void MaskSettings(Dictionary<string, string>? settings, TransportDescriptor? descriptor)
    {
        if (settings is null || settings.Count == 0)
        {
            return;
        }

        var secretKeys = TransportDescriptors.SecretKeys(descriptor);
        foreach (var key in settings.Keys.ToList())
        {
            if (!string.IsNullOrEmpty(settings[key]) && (descriptor is null || secretKeys.Contains(key)))
            {
                settings[key] = SourceKinds.SecretMask;
            }
        }
    }

    /// <summary>The "a written *** means keep the stored value" half for a <c>Settings</c> bag, matched by
    /// KEY. Needs no descriptor: only a value that IS the mask is restored, and only when the stored bag
    /// has that key — so a kind whose plugin has since been uninstalled still round-trips.</summary>
    private static void MergeSettings(Dictionary<string, string>? incoming, IReadOnlyDictionary<string, string>? stored)
    {
        if (incoming is null || stored is null)
        {
            return;
        }

        foreach (var key in incoming.Keys.ToList())
        {
            if (incoming[key] == SourceKinds.SecretMask && stored.TryGetValue(key, out var storedValue))
            {
                incoming[key] = storedValue;
            }
        }
    }

    /// <summary>Plan 009 A1.2: masks Hash/Salt on every configured push key — the doc comment on
    /// <c>IngestKey</c> itself calls this out explicitly ("Read-back through the REST layer masks
    /// Hash/Salt with SourceKinds.SecretMask"), same convention as the connector secrets above.</summary>
    private static void MaskIngestKeys(IngestConfig? ingest)
    {
        if (ingest is null)
        {
            return;
        }

        foreach (var key in ingest.Keys)
        {
            if (!string.IsNullOrEmpty(key.Hash))
            {
                key.Hash = SourceKinds.SecretMask;
            }

            if (!string.IsNullOrEmpty(key.Salt))
            {
                key.Salt = SourceKinds.SecretMask;
            }
        }
    }

    /// <summary>Deep clones <paramref name="incoming"/> and replaces every secret value that equals
    /// <see cref="SourceKinds.SecretMask"/> with the corresponding value from
    /// <paramref name="stored"/> (D-H: "a written *** value means keep the stored value") —
    /// protects the SPA's GET (masked) → edit → PUT (whole object) cycle from clobbering real
    /// secrets with the literal string "***". If <paramref name="stored"/> is null, or has no
    /// connector, or the specific stored value doesn't exist (e.g. a header key present in
    /// <paramref name="incoming"/> but absent from <paramref name="stored"/> — nothing to
    /// substitute), the masked "***" is left as-is: there is nothing to "keep". Never mutates
    /// either input.</summary>
    public static SourceDefinition MergeSecrets(SourceDefinition incoming, SourceDefinition? stored)
    {
        var clone = ConfigJsonMapper.DeepCloneModel(incoming);
        MergeIngestKeys(clone.Ingest, stored?.Ingest);

        var connector = clone.Connector;
        var storedConnector = stored?.Connector;
        if (connector is null || storedConnector is null)
        {
            return clone;
        }

        if (connector.Url is { } url && storedConnector.Url is { } storedUrl)
        {
            foreach (var key in url.Headers.Keys.ToList())
            {
                if (url.Headers[key] == SourceKinds.SecretMask && storedUrl.Headers.TryGetValue(key, out var storedValue))
                {
                    url.Headers[key] = storedValue;
                }
            }
        }

        // Plan 010: the "a written *** means keep the stored value" rule, applied to every [Secret] slot
        // reachable from the connector — the incoming and stored graphs are walked in lockstep by property
        // name. HasStored being false means the stored graph had no corresponding object at all (e.g. the
        // kind changed), so there is nothing to keep and the mask is left standing.
        foreach (var slot in SecretWalk.Slots(connector, storedConnector))
        {
            if (slot.Value == SourceKinds.SecretMask && slot.HasStored)
            {
                slot.Set(slot.StoredValue);
            }
        }

        MergeSettings(connector.Settings, storedConnector.Settings);

        return clone;
    }

    /// <summary>Plan 009 A1.2: restores Hash/Salt (matched by <see cref="IngestKey.Id"/>) for any
    /// incoming key whose Hash/Salt is the literal mask — same "a written *** means keep the stored
    /// value" rule the connector secrets follow, so the SPA's GET (masked) -> edit -> PUT (whole
    /// object) round-trip for a source's OTHER fields never clobbers real key hashes with "***". A
    /// key present in <paramref name="incoming"/> but absent from <paramref name="stored"/> (freshly
    /// generated via POST .../ingest/keys, or added directly) is left as-is — there is nothing stored
    /// to restore from, exactly like an unmatched header key above.</summary>
    private static void MergeIngestKeys(IngestConfig? incoming, IngestConfig? stored)
    {
        if (incoming is null || stored is null)
        {
            return;
        }

        var storedById = stored.Keys.ToDictionary(k => k.Id, k => k);
        foreach (var key in incoming.Keys)
        {
            if (!storedById.TryGetValue(key.Id, out var storedKey))
            {
                continue;
            }

            if (key.Hash == SourceKinds.SecretMask)
            {
                key.Hash = storedKey.Hash;
            }

            if (key.Salt == SourceKinds.SecretMask)
            {
                key.Salt = storedKey.Salt;
            }
        }
    }

    /// <summary>True if any secret slot (URL header value, gRPC password/token, plan 009 A1.2:
    /// Ingest.Keys[].Hash/.Salt) on <paramref name="def"/> currently holds the literal mask value
    /// <see cref="SourceKinds.SecretMask"/>. Used by <see cref="ImportPlanner"/> to decide whether an
    /// imported source entity needs <see cref="MergeSecrets"/> applied before comparing it against the
    /// stored catalog entity.</summary>
    public static bool HasMaskedValues(SourceDefinition def)
    {
        if (def.Ingest is { } ingest && ingest.Keys.Any(k => k.Hash == SourceKinds.SecretMask || k.Salt == SourceKinds.SecretMask))
        {
            return true;
        }

        var connector = def.Connector;
        if (connector is null)
        {
            return false;
        }

        if (connector.Url is { } url && url.Headers.Values.Any(v => v == SourceKinds.SecretMask))
        {
            return true;
        }

        if (connector.Settings.Values.Any(v => v == SourceKinds.SecretMask))
        {
            return true;
        }

        return SecretWalk.Slots(connector).Any(s => s.Value == SourceKinds.SecretMask);
    }

    // ------------------------------------------------------------------
    // Plan 009 B2: SinkSpec.Nats credentials — PipelineDefinition.Sinks / TableDefinition.Sinks.
    // A separate region (new methods, not folded into the SourceDefinition-shaped ones above) because
    // Sinks lives on PipelineDefinition/TableDefinition, not SourceDefinition; the masking RULE is the
    // same secrets-lite convention (D-H) the rest of this file implements.
    // ------------------------------------------------------------------

    /// <summary>Deep-clones <paramref name="sinks"/> and replaces every NON-EMPTY
    /// <c>NatsPubConfig</c> credential (Token/Password/Credentials — Username is treated as an
    /// identifier, not a secret, matching <see cref="GrpcSubConfig"/>'s existing convention of masking
    /// Password/Token but not Username) with <see cref="SourceKinds.SecretMask"/>. Never mutates
    /// <paramref name="sinks"/>.</summary>
    public static List<SinkSpec> MaskSinks(List<SinkSpec> sinks)
    {
        var clone = ConfigJsonMapper.DeepCloneModel(sinks);
        MaskSinksInPlace(clone);
        return clone;
    }

    /// <summary>Deep-clones the WHOLE <paramref name="def"/> (every field, via
    /// <see cref="ConfigJsonMapper.DeepCloneModel{T}"/> — same "everything, not a hand-picked field
    /// list" approach <see cref="Mask(SourceDefinition)"/> uses) and masks its Sinks credentials —
    /// the PipelineDefinition read-path counterpart of <see cref="Mask(SourceDefinition)"/>. Never
    /// mutates <paramref name="def"/>.</summary>
    public static PipelineDefinition MaskPipeline(PipelineDefinition def)
    {
        var clone = ConfigJsonMapper.DeepCloneModel(def);
        MaskSinksInPlace(clone.Sinks);
        return clone;
    }

    /// <summary>TableDefinition counterpart of <see cref="MaskPipeline"/>.</summary>
    public static TableDefinition MaskTable(TableDefinition def)
    {
        var clone = ConfigJsonMapper.DeepCloneModel(def);
        MaskSinksInPlace(clone.Sinks);
        return clone;
    }

    /// <summary>Masks NatsPubConfig credentials on an ALREADY-owned (already cloned, or freshly
    /// constructed) list in place — the shared primitive <see cref="MaskSinks"/>/
    /// <see cref="MaskPipeline"/>/<see cref="MaskTable"/> all call after cloning, so the clone-then-mask
    /// sequence exists exactly once.</summary>
    private static void MaskSinksInPlace(List<SinkSpec> sinks)
    {
        foreach (var slot in sinks.SelectMany(sink => SecretWalk.Slots(sink)))
        {
            if (!string.IsNullOrEmpty(slot.Value))
            {
                slot.Set(SourceKinds.SecretMask);
            }
        }

        foreach (var sink in sinks)
        {
            MaskSettings(sink.Settings, TransportDescriptors.ForSink(sink.Kind));
        }
    }

    /// <summary>Restores masked NatsPubConfig credentials in <paramref name="incoming"/> from
    /// <paramref name="stored"/> — the Sinks counterpart of <see cref="MergeSecrets"/>. Sinks has no
    /// stable id (see <c>SinkSpec</c>'s own doc comment — the container shape exists so a second sink
    /// KIND is additive, not so entries can be individually addressed), so entries are matched
    /// POSITIONALLY by list index; this is the same "PUT replaces the whole object" model the rest of
    /// this API already uses for Tags/Metadata, so a client that GETs (masked) then PUTs the whole array
    /// back preserves index alignment for free. An index present only in <paramref name="incoming"/>
    /// (list grew) has nothing to restore from and is left as-is — same "nothing to keep" rule
    /// <see cref="MergeSecrets"/> applies to an unmatched header key. Never mutates either input.</summary>
    public static List<SinkSpec> MergeSinkSecrets(List<SinkSpec> incoming, List<SinkSpec>? stored)
    {
        var clone = ConfigJsonMapper.DeepCloneModel(incoming);
        if (stored is null)
        {
            return clone;
        }

        for (var i = 0; i < clone.Count && i < stored.Count; i++)
        {
            foreach (var slot in SecretWalk.Slots(clone[i], stored[i]))
            {
                if (slot.Value == SourceKinds.SecretMask && slot.HasStored)
                {
                    slot.Set(slot.StoredValue);
                }
            }

            MergeSettings(clone[i].Settings, stored[i].Settings);
        }

        return clone;
    }

    /// <summary>True if any NatsPubConfig credential slot across <paramref name="sinks"/> currently
    /// holds the literal mask value. Used the same way <see cref="HasMaskedValues"/> is used for
    /// sources: decide whether an imported/PUT-ed Sinks list needs <see cref="MergeSinkSecrets"/>
    /// applied before comparing/persisting it.</summary>
    public static bool HasMaskedSinkValues(List<SinkSpec>? sinks) =>
        (sinks ?? []).Any(s => s.Settings.Values.Any(v => v == SourceKinds.SecretMask))
        || (sinks ?? []).SelectMany(s => SecretWalk.Slots(s)).Any(slot => slot.Value == SourceKinds.SecretMask);
}
