using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Xunit;

namespace StreamsForge.Dapr.Live.Tests;

/// <summary>
/// Plan 025 D3.6: plan 015's entitlements/approvals/audit actor path (<c>AccessPolicyActor</c>,
/// <c>AuditLogActor</c>) is listed in <c>dapr/PARITY.md</c> as "explicitly NOT debt — checked and present
/// on Dapr" but also, in the very next section, as entirely unexercised live on this flavor ("the Dapr
/// access/approval/audit actor path is entirely unexercised"). This is that exercise: one Deny grant, one
/// 403, one revoke, one 200, and one audit read proving the grant CHANGE itself was recorded — the same
/// round trip <c>.claude/skills/sf-access/SKILL.md</c> documents against the Orleans flavor, run here for
/// the first time against a real Dapr instance.
///
/// <para>Uses <see cref="Actions.TableWrite"/>'s wire value, <c>"table.write"</c> — the actual action
/// <c>PUT /api/tables/{id}</c> is gated on (<c>TablesEndpoints.cs</c>'s <c>Actions.TableWrite</c> check),
/// not the informal <c>"table.update"</c> the plan brief mentions; there is no <c>table.update</c> action
/// in <c>AccessModels.cs</c>, and getting the exact string right is the whole point of a live check like
/// this one.</para>
/// </summary>
[Collection(DaprLiveTestCollection.Name)]
public sealed class AccessAuditTests : IAsyncLifetime
{
    private DaprHostProcess? _host;
    private string? _skipReason;

    public async Task InitializeAsync()
    {
        _skipReason = DaprHostProcess.Preflight();
        if (_skipReason is not null)
        {
            return;
        }

        await DaprHostProcess.ResetAsync();
        _host = new DaprHostProcess("access-audit");
    }

    public async Task DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }
    }

    [Fact]
    public async Task Access_deny_grant_is_enforced_and_audited()
    {
        Assert.True(_skipReason is null, $"skipped: {_skipReason}");
        var host = _host!;

        host.Start();
        await host.WaitHealthyAsync();

        using var admin = await host.LoginAsync(DaprHostProcess.AdminUser, DaprHostProcess.AdminPassword);
        using var editor = await host.LoginAsync("editor", "editor123!");

        var tables = await admin.GetFromJsonAsync<List<TableSummaryDto>>($"{host.BaseUrl}/api/tables");
        var positions = tables?.First(t => t.Name == "positions")
            ?? throw new InvalidOperationException("seeded table 'positions' not found");

        // A no-op PUT: same name/description/sql the table already has. editor holds the Editor role
        // (table.write at * via BuiltInRoleCatalog), so this must succeed BEFORE any Deny grant exists —
        // establishing the baseline the rest of the test's before/after comparison depends on.
        var baseline = await PutPositionsAsync(editor, host.BaseUrl, positions);
        Assert.True(baseline.IsSuccessStatusCode, $"baseline PUT (no grant yet) -> {(int)baseline.StatusCode}, expected success");

        // Attach a Deny grant for table.write at scope "*" directly on the editor user's own access
        // entry — PUT /api/access/users/{u} is a whole-object replace, so roles must be resent or the
        // user loses them (sf-access's own documented gotcha).
        var grantResp = await admin.PutAsJsonAsync($"{host.BaseUrl}/api/access/users/editor", new
        {
            roles = new[] { "Editor" },
            grants = new[]
            {
                new { action = "table.write", scope = "*", effect = "Deny", note = "plan 025 D3.6 live check" },
            },
        });
        var grantText = await grantResp.Content.ReadAsStringAsync();
        Assert.True(grantResp.IsSuccessStatusCode, $"PUT access/users/editor (deny) -> {(int)grantResp.StatusCode}: {grantText}");

        // Auth:PolicyCacheSeconds defaults to 10 — poll rather than assume the deny is visible instantly.
        var denied = await PollUntilStatusAsync(
            () => PutPositionsAsync(editor, host.BaseUrl, positions),
            HttpStatusCode.Forbidden,
            TimeSpan.FromSeconds(15));
        Assert.Equal(HttpStatusCode.Forbidden, denied);

        // Revoke: put the editor entry back with no direct grants.
        var revokeResp = await admin.PutAsJsonAsync($"{host.BaseUrl}/api/access/users/editor", new
        {
            roles = new[] { "Editor" },
            grants = Array.Empty<object>(),
        });
        Assert.True(revokeResp.IsSuccessStatusCode, $"PUT access/users/editor (revoke) -> {(int)revokeResp.StatusCode}");

        var allowedAgain = await PollUntilStatusAsync(
            () => PutPositionsAsync(editor, host.BaseUrl, positions),
            HttpStatusCode.OK,
            TimeSpan.FromSeconds(15));
        Assert.Equal(HttpStatusCode.OK, allowedAgain);

        // The audit log recorded the grant CHANGE (the PUT to /api/access/users/editor itself, an
        // access.write mutation) on today's UTC shard.
        var day = DateTime.UtcNow.ToString("yyyyMMdd");
        var auditResp = await admin.GetAsync($"{host.BaseUrl}/api/audit/{day}?action=access.write&limit=200");
        var auditText = await auditResp.Content.ReadAsStringAsync();
        Assert.True(auditResp.IsSuccessStatusCode, $"GET /api/audit/{day} -> {(int)auditResp.StatusCode}: {auditText}");
        using var auditDoc = JsonDocument.Parse(auditText);
        var entries = auditDoc.RootElement.GetProperty("entries").EnumerateArray().ToList();
        Assert.True(
            entries.Any(e => e.GetProperty("actor").GetString() == "admin"
                           && e.GetProperty("action").GetString()!.StartsWith("access.", StringComparison.Ordinal)),
            $"no access.write audit entry by 'admin' found on day {day}: {auditText}");
    }

    private static async Task<HttpResponseMessage> PutPositionsAsync(HttpClient client, string baseUrl, TableSummaryDto table)
    {
        return await client.PutAsJsonAsync($"{baseUrl}/api/tables/{table.Id}", new
        {
            name = table.Name,
            description = table.Description,
            sql = table.Sql,
        });
    }

    private static async Task<HttpStatusCode> PollUntilStatusAsync(
        Func<Task<HttpResponseMessage>> attempt, HttpStatusCode expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        HttpStatusCode last = default;
        while (DateTime.UtcNow < deadline)
        {
            using var resp = await attempt();
            last = resp.StatusCode;
            if (last == expected)
            {
                return last;
            }
            await Task.Delay(500);
        }
        return last;
    }

    private sealed record TableSummaryDto(string Id, string Name, string Description, string Sql);
}
