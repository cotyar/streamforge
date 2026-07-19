using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using StreamForge.Abstractions;
using StreamForge.Host.Grpc.Dynamic;

namespace StreamForge.Api;

public static class SourcesEndpoints
{
    public static void MapSourcesEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/sources");

        group.MapGet("/", async (ICatalogFacade registry) =>
            Results.Ok(await registry.GetSourcesAsync())
        ).RequireAuthorization("Viewer");

        group.MapGet("/{name}", async (string name, ICatalogFacade registry) =>
        {
            var src = await registry.GetSourceAsync(name);
            return src is null ? Results.NotFound() : Results.Ok(src);
        }).RequireAuthorization("Viewer");

        // Downloadable, self-contained .proto for this source: DescriptorFactory's schema plus the
        // DynamicStreamService streaming contract, ready for a client to compile standalone (see
        // tools/generate-client.sh). Field numbers always come from ICatalogFacade.EnsureFieldNumbersAsync
        // so they stay identical to what the dynamic gRPC reflection surface uses.
        group.MapGet("/{name}/proto", async (string name, ICatalogFacade registry) =>
        {
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

        group.MapPost("/", async (SourceDefinition def, ICatalogFacade registry) =>
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

            if (await registry.GetSourceAsync(def.Name) is not null)
            {
                return Results.BadRequest(new ErrorResponse("source name already exists"));
            }

            await registry.UpsertSourceAsync(def);
            return Results.Created($"/api/sources/{def.Name}", def);
        }).RequireAuthorization("Editor");

        group.MapPut("/{name}", async (string name, SourceDefinition def, ICatalogFacade registry) =>
        {
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

        group.MapDelete("/{name}", async (string name, ICatalogFacade registry) =>
        {
            var removed = await registry.DeleteSourceAsync(name);
            return removed ? Results.NoContent() : Results.NotFound();
        }).RequireAuthorization("Editor");
    }
}
