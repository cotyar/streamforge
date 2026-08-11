using StreamForge.Abstractions;

namespace StreamForge.AppCore.Config;

/// <summary>
/// Plan 006 (D-H): secrets-lite masking for the two secret slots on a connector-kind source —
/// <see cref="UrlPollConfig.Headers"/> VALUES and <see cref="GrpcSubConfig.Password"/> /
/// <see cref="GrpcSubConfig.Token"/>. Secrets are stored plaintext at rest (no KMS, no encryption —
/// documented ceiling) and masked as <see cref="SourceKinds.SecretMask"/> ("***") on every read
/// path (GET/list/export); a written "***" value means "keep the stored value" — see
/// <see cref="MergeSecrets"/>, which implements that half of the contract for the PUT-replaces-
/// whole-object GET→edit→PUT cycle.
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

        if (connector.Grpc is { } grpc)
        {
            if (!string.IsNullOrEmpty(grpc.Password))
            {
                grpc.Password = SourceKinds.SecretMask;
            }

            if (!string.IsNullOrEmpty(grpc.Token))
            {
                grpc.Token = SourceKinds.SecretMask;
            }
        }

        // Plan 009 B1: Token/Password/Credentials are secrets (a .creds file's contents are as
        // sensitive as a password); Url/Subject/QueueGroup/Format/Username are not.
        if (connector.Nats is { } nats)
        {
            if (!string.IsNullOrEmpty(nats.Token))
            {
                nats.Token = SourceKinds.SecretMask;
            }

            if (!string.IsNullOrEmpty(nats.Password))
            {
                nats.Password = SourceKinds.SecretMask;
            }

            if (!string.IsNullOrEmpty(nats.Credentials))
            {
                nats.Credentials = SourceKinds.SecretMask;
            }
        }

        return clone;
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

        if (connector.Grpc is { } grpc && storedConnector.Grpc is { } storedGrpc)
        {
            if (grpc.Password == SourceKinds.SecretMask)
            {
                grpc.Password = storedGrpc.Password;
            }

            if (grpc.Token == SourceKinds.SecretMask)
            {
                grpc.Token = storedGrpc.Token;
            }
        }

        // Plan 009 B1 — same "a written *** means keep the stored value" rule as Grpc above.
        if (connector.Nats is { } nats && storedConnector.Nats is { } storedNats)
        {
            if (nats.Token == SourceKinds.SecretMask)
            {
                nats.Token = storedNats.Token;
            }

            if (nats.Password == SourceKinds.SecretMask)
            {
                nats.Password = storedNats.Password;
            }

            if (nats.Credentials == SourceKinds.SecretMask)
            {
                nats.Credentials = storedNats.Credentials;
            }
        }

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

        if (connector.Grpc is { } grpc && (grpc.Password == SourceKinds.SecretMask || grpc.Token == SourceKinds.SecretMask))
        {
            return true;
        }

        if (connector.Nats is { } nats &&
            (nats.Token == SourceKinds.SecretMask || nats.Password == SourceKinds.SecretMask || nats.Credentials == SourceKinds.SecretMask))
        {
            return true;
        }

        return false;
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
        foreach (var sink in sinks)
        {
            if (sink.Nats is not { } nats)
            {
                continue;
            }

            if (!string.IsNullOrEmpty(nats.Token))
            {
                nats.Token = SourceKinds.SecretMask;
            }

            if (!string.IsNullOrEmpty(nats.Password))
            {
                nats.Password = SourceKinds.SecretMask;
            }

            if (!string.IsNullOrEmpty(nats.Credentials))
            {
                nats.Credentials = SourceKinds.SecretMask;
            }
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
            if (clone[i].Nats is not { } nats || stored[i].Nats is not { } storedNats)
            {
                continue;
            }

            if (nats.Token == SourceKinds.SecretMask)
            {
                nats.Token = storedNats.Token;
            }

            if (nats.Password == SourceKinds.SecretMask)
            {
                nats.Password = storedNats.Password;
            }

            if (nats.Credentials == SourceKinds.SecretMask)
            {
                nats.Credentials = storedNats.Credentials;
            }
        }

        return clone;
    }

    /// <summary>True if any NatsPubConfig credential slot across <paramref name="sinks"/> currently
    /// holds the literal mask value. Used the same way <see cref="HasMaskedValues"/> is used for
    /// sources: decide whether an imported/PUT-ed Sinks list needs <see cref="MergeSinkSecrets"/>
    /// applied before comparing/persisting it.</summary>
    public static bool HasMaskedSinkValues(List<SinkSpec>? sinks) =>
        (sinks ?? []).Any(s => s.Nats is { } n &&
            (n.Token == SourceKinds.SecretMask || n.Password == SourceKinds.SecretMask || n.Credentials == SourceKinds.SecretMask));
}
