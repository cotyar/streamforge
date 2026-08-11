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

        return false;
    }
}
