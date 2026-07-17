using System.Text;
using Orleans;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;

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

        // Downloadable, self-contained .proto for this source: DescriptorFactory's schema plus the
        // DynamicStreamService streaming contract, ready for a client to compile standalone (see
        // tools/generate-client.sh). Field numbers always come from IRegistryGrain.EnsureFieldNumbersAsync
        // so they stay identical to what the dynamic gRPC reflection surface uses.
        group.MapGet("/{name}/proto", async (string name, IClusterClient client) =>
        {
            var registry = Registry(client);
            var src = await registry.GetSourceAsync(name);
            if (src is null)
            {
                return Results.NotFound();
            }

            var numbersJson = await registry.EnsureFieldNumbersAsync(EntitySchemas.SourceKey(src.Name), src.Fields);
            var numbers = EntitySchemas.ParseMap(numbersJson);
            var schema = DescriptorFactory.Generate(src.Name, src.Fields, numbers);
            var protoText = ProtoFileBuilder.Build("source", src.Name, schema);

            return Results.File(Encoding.UTF8.GetBytes(protoText), "text/plain; charset=utf-8", schema.FileProto.Name);
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
