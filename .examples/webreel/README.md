# StreamsForge console recordings

These are reproducible product recordings, not animated screenshots. They target a
seeded local Orleans console at http://127.0.0.1:5199 and use the documented demo
credentials (admin / admin123!).

Start the console in a separate terminal:

    ~/.dotnet/dotnet run --project orleans/src/StreamsForge.Host

Then record a GIF, MP4, or WebM with Webreel:

    bunx webreel record -c .examples/webreel/streamsforge-console.config.json console-tour

The command writes videos/console-tour.gif plus the configured PNG checkpoints.
videos/ is intentionally ignored: commit only approved, hand-reviewed product media
to landing/public/media/.

Webreel was fetched for reference via opensrc fetch vercel-labs/webreel. Its upstream
checkout currently has a broken Git-LFS sample-video object, but the CLI schema and the
recording flow used here are from the checked-out source and README.
