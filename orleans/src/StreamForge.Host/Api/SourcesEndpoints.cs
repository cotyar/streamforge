using Orleans;
using StreamForge.Abstractions;

namespace StreamForge.Host.Api;

public static class SourcesEndpoints
{
    public static void MapSourcesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        group.MapGet("/", async (IClusterClient client) =>
            Results.Ok(await Registry(client).GetSourcesAsync())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{name}", async (string name, IClusterClient client) =>
        {
            var src = await Registry(client).GetSourceAsync(name);
            return src is null ? Results.NotFound() : Results.Ok(src);
        }).RequireAuthorization("Viewer");

        group.MapPost("/", async (SourceDefinition def, IClusterClient client) =>
        {
            if (string.IsNullOrWhiteSpace(def.Name))
            {
                return Results.BadRequest(new ErrorResponse("name is required"));
            }

            if (def.Fields.Count == 0)
            {
                return Results.BadRequest(new ErrorResponse("at least one field is required"));
            }

            if (def.EventsPerSecond <= 0)
            {
                return Results.BadRequest(new ErrorResponse("eventsPerSecond must be > 0"));
            }

            var registry = Registry(client);
            if (await registry.GetSourceAsync(def.Name) is not null)
            {
                return Results.BadRequest(new ErrorResponse("source name already exists"));
            }

            await registry.UpsertSourceAsync(def);
            return Results.Created($"/api/sources/{def.Name}", def);
        }).RequireAuthorization("Editor");

        group.MapPut("/{name}", async (string name, SourceDefinition def, IClusterClient client) =>
        {
            var registry = Registry(client);
            if (await registry.GetSourceAsync(name) is null)
            {
                return Results.NotFound();
            }

            if (def.Fields.Count == 0)
            {
                return Results.BadRequest(new ErrorResponse("at least one field is required"));
            }

            if (def.EventsPerSecond <= 0)
            {
                return Results.BadRequest(new ErrorResponse("eventsPerSecond must be > 0"));
            }

            def.Name = name;
            await registry.UpsertSourceAsync(def);
            return Results.Ok(def);
        }).RequireAuthorization("Editor");

        group.MapDelete("/{name}", async (string name, IClusterClient client) =>
        {
            var removed = await Registry(client).DeleteSourceAsync(name);
            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");
    }

    private static IRegistryGrain Registry(IClusterClient client) =>
        client.GetGrain<IRegistryGrain>(StreamConstants.RegistryKey);
}
