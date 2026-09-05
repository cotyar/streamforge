using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// The REST plumbing wave 2's five test classes share: post-and-assert-2xx, get-and-assert-2xx, poll to
/// a deadline, and the one entity shape (<c>id Long / seq Long / value String</c>, dedup key <c>id</c>)
/// every exact-count scenario in this project uses.
///
/// <para>Wave 1's classes each carry private copies of the first three (see
/// <c>SourceExactCountTests</c>'s <c>PostOkAsync</c>/<c>GetRowsAsync</c>). That was right for three
/// classes; at eight it stops being right, because the copies had already begun to differ in what they
/// attach to a failure. They are deliberately NOT retro-fitted onto wave 1's classes here — this
/// project's rule is that a live test's diagnostics are part of what it proves, and rewriting a passing
/// live test to share a helper is a change with no gate behind it. New classes use this; old classes
/// keep theirs.</para>
///
/// <para><b>Everything here speaks the wire.</b> No in-process type from either host is referenced (the
/// csproj's ProjectReference is <c>ReferenceOutputAssembly="false"</c> for exactly that reason), so a
/// serialization regression cannot hide behind a shared object graph.</para>
/// </summary>
public static class LiveRest
{
    /// <summary>The shared row shape: <c>id</c>/<c>seq</c> Long, <c>value</c> String.</summary>
    public static object[] Fields() =>
    [
        new { name = "id", type = "Long" },
        new { name = "seq", type = "Long" },
        new { name = "value", type = "String" },
    ];

    /// <summary>The mapping document for <see cref="Fields"/>. <c>fields</c> is REQUIRED, not optional:
    /// a mapping that sets <c>dedupKeyField</c> without listing the fields is refused with a 400 at the
    /// API boundary (the dedup key must name an EMITTED field, and with no field list there are none) —
    /// a shape that cost wave 1 a debugging round and is therefore written down here rather than
    /// rediscovered.</summary>
    public static object Mapping() => new
    {
        itemsPath = "$",
        dedupKeyField = "id",
        fields = Fields().Select(f => (object)new { field = f }).ToArray(),
    };

    public static string NdjsonLine(int id) => $"{{\"id\":{id},\"seq\":{id},\"value\":\"v{id}\"}}";

    public static string Ndjson(IEnumerable<int> ids) => string.Concat(ids.Select(id => NdjsonLine(id) + "\n"));

    public static async Task<JsonDocument> PostOkAsync(HttpClient client, string url, object body)
    {
        var resp = await client.PostAsJsonAsync(url, body);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"POST {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    public static async Task<JsonDocument> GetJsonAsync(HttpClient client, string url)
    {
        var resp = await client.GetAsync(url);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.IsSuccessStatusCode, $"GET {url} -> {(int)resp.StatusCode}: {text}");
        return JsonDocument.Parse(text);
    }

    /// <summary>Polls to a deadline and, on timeout, fails with the offending host's own drained log
    /// tail attached. Without the tail a live failure is indistinguishable between "the source never
    /// emitted", "the consumer never subscribed" and "the entity never started".
    ///
    /// <para><b>The message is a delegate, not a string</b>, so it can name the value the poll ACTUALLY
    /// last saw. An interpolated string argument would be built before the first poll ran and would
    /// report the initial value forever — a diagnostic that lies is worse than none.</para></summary>
    public static async Task PollAsync(TimeSpan timeout, Func<Task<bool>> condition, Func<string> message, Func<string> logTail)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(250);
        }
        Assert.Fail($"{message()} within {timeout.TotalSeconds:0}s.\n--- host log tail ---\n{logTail()}");
    }

    /// <summary>One read of <c>GET /api/tables/{id}/rows</c>: the total and the set of <c>id</c> values.
    /// <c>totalRows</c> and the returned array are read from the SAME response, so "500 rows, ids
    /// 0..499" is one atomic observation rather than two reads that could straddle a delta.</summary>
    public static async Task<TableRows> RowsAsync(HttpClient client, string baseUrl, string tableId)
    {
        using var doc = await GetJsonAsync(client, $"{baseUrl}/api/tables/{tableId}/rows?limit=5000");
        var total = doc.RootElement.GetProperty("totalRows").GetInt32();
        var ids = doc.RootElement.GetProperty("rows").EnumerateArray()
            .Select(r => r.GetProperty("row").GetProperty("id").GetInt64())
            .ToHashSet();
        return new TableRows(total, ids);
    }

    /// <summary>Asserts the table holds EXACTLY the given ids — no gaps and no extras, which is what
    /// separates "the count happens to match" from "every row landed once".</summary>
    public static void AssertIdSet(TableRows rows, IEnumerable<int> expectedIds)
    {
        var expected = expectedIds.Select(i => (long)i).ToHashSet();
        Assert.Equal(expected.Count, rows.Ids.Count);
        var missing = expected.Where(i => !rows.Ids.Contains(i)).Take(10).ToList();
        Assert.True(missing.Count == 0, $"ids missing from the table: {string.Join(",", missing)}");
        var extra = rows.Ids.Where(i => !expected.Contains(i)).Take(10).ToList();
        Assert.True(extra.Count == 0, $"unexpected ids in the table: {string.Join(",", extra)}");
    }

    /// <summary>Polls the table until it holds exactly <paramref name="expected"/> rows, then waits a
    /// settling interval and re-reads — the second read is what would catch an OVER-count (a duplicate
    /// arriving just after the target was hit), which a poll that returns on first match cannot.</summary>
    public static async Task<TableRows> SettledRowsAsync(
        HttpClient client, string baseUrl, string tableId, int expected, TimeSpan timeout, Func<string> logTail)
    {
        var deadline = DateTime.UtcNow + timeout;
        TableRows last = new(-1, []);
        var hit = false;
        while (DateTime.UtcNow < deadline)
        {
            last = await RowsAsync(client, baseUrl, tableId);
            if (last.TotalRows == expected)
            {
                hit = true;
                break;
            }
            await Task.Delay(250);
        }
        // The loop is written out rather than delegated to PollAsync because the failure message has to
        // name the count actually REACHED, which is only known after the loop — a string built at the
        // call site would report the count from before the first poll.
        Assert.True(
            hit,
            $"table {tableId} reached {last.TotalRows} rows, expected exactly {expected}, within "
          + $"{timeout.TotalSeconds:0}s.\n--- host log tail ---\n{logTail()}");

        await Task.Delay(1500);
        return await RowsAsync(client, baseUrl, tableId);
    }

    public sealed record TableRows(int TotalRows, HashSet<long> Ids);
}
