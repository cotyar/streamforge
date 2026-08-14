# Plan 012 — CSV in, CSV out

Status: **DONE**. Baseline `7fa27e1` (Orleans 1640 tests, Dapr 313, both green).

Request: *"Make sources able to work with csv and similar files on input. And also make csv supported on
the output side as well."*

## What was already true, and what wasn't

CSV input existed — `FormatParsers.ParseCsv` is a real RFC 4180 tokenizer (quoted fields, embedded commas
and newlines, `""` escapes, CRLF/LF/CR) and the `file`, `folder` and `nats` source kinds all take
`format: "csv"`. Two things were missing, and the output side was missing entirely:

| Gap | Fix |
|---|---|
| A `url` source could only read JSON — `UrlPollConfig` had no `Format` at all, so an endpoint serving `text/csv` needed a file in between | `UrlPollConfig.Format` (`[Id(3)]`, default `"json"`), routed through the same parse path file/folder/message payloads use |
| "csv" meant *comma* — a TSV or a semicolon-separated export (what Excel writes in a decimal-comma locale) parsed as one wide column | The delimiter is sniffed from the header line: `,` `\t` `;` `\|`, ties to the earlier one, none found = comma |
| Nothing anywhere could WRITE CSV | A `file` sink kind, plus CSV downloads for a table's rows and a pipeline's recent results |

## Egress: the `file` sink kind

`SinkKinds.File` + `FileSinkConfig` (path, format, optional explicit columns) + `FileSinkTransport` and
`FileSinkClient`, registered in `SinkTransports` — one class and one registry line, exactly the
[TRANSPORTS.md](../TRANSPORTS.md) recipe plan 010 built, and the first sink that isn't a broker. Both
flavors get it for free (Orleans' `NatsPublisherService` and Dapr's `NatsSinkPublisherService` both go
through the registry), and the console form is generated from the descriptor with no `web/` change.

It holds the same fire-and-forget contract `NatsSinkClient` documents — never throws, never blocks past a
3s timeout, counts and throttles its own failures — and adds three limits that are stated rather than
discovered:

- **Append, never truncate.** The file is a log. A sink pointed at the wrong path costs junk at the end of
  a file, not its contents. A restart continues an existing file, reusing its header verbatim.
- **The CSV header is fixed for the life of the file.** Columns come from `Columns` if set, else the
  existing file's header, else the first row written. A column appearing only in a LATER row is dropped
  **and counted** (`SinkPublishCounters.LastError` names it) — a row with an extra cell would shift every
  column for every reader downstream.
- **No rotation, no size cap, no fsync**, and on Unix a file rotated underneath a running sink keeps
  receiving writes on the old inode. Writes land on the HOST's filesystem as the host process user — the
  same trust the `file`/`folder` SOURCE kinds already extend to an Editor, in the write direction. In a
  container the path must be a mounted volume.

A table delta's weight is written as a `_weight` column: a Z-set delta stream without its weights is not
the table, since a −1 retraction would be indistinguishable from an insert.

## Egress: the two download routes

`GET /api/tables/{id}/rows.csv` and `GET /api/pipelines/{id}/results.csv` (Viewer, `text/csv`, capped at
100 000 rows, default 10 000), with a **CSV** button on each detail page. Own routes rather than a
`?format=csv` branch: `/rows` is `.Produces<TableRowsResponse>()` and the per-entity OpenAPI document
rewrites its row shape into the table's real output schema, which a second content type on the same
operation would have muddied.

Table columns come from the compiled `OutputFields` (stable across exports even when a page of rows
happens to be missing a value), plus `_weight`. A pipeline has no stored output schema, so its header is
the union of the keys present, first-seen order.

`CsvFormatter` is the single writer behind both the sink and the routes, so a table exported by hand and
the same table written by a sink are byte-identical.

## Verification

- Orleans **1676** tests / Dapr **313**, both green; no pre-existing test file modified.
- The property that matters for a format meant to survive a spreadsheet: **anything the formatter writes,
  the parser reads back** — pinned in `CsvIoTests` over embedded delimiters, quotes, newlines and empty
  cells.
- `FileSinkClientTests` exercises the real success path against real files (unlike the NATS sink, whose
  tests can only reach its failure path here): header written once, restart reuses an existing header
  *including a different column order*, ragged rows keep a constant line width, an unwritable path counts
  instead of throwing.
- Live on an isolated instance (ports 7611/7612 + a throwaway `python3 -m http.server` on 7613, temp data
  dir, all killed afterwards): a url source with `format: "csv"` polled a **tab**-separated body over HTTP
  and produced correctly typed rows (`qty` summing as a number, `price` 10.5 as a double) with no
  delimiter configured anywhere; a table with a `file` sink wrote `/tmp/.../csv_positions.csv`;
  `GET /rows.csv` returned `text/csv` with a `Content-Disposition` filename and 401 without a token;
  `GET /results.csv` did the same for a pipeline.

## One asymmetry worth knowing about

A table's **sink** writes the DELTA STREAM — the same thing a NATS sink publishes, so a change to a group
appears as a `-1` retraction followed by a `+1` insert, and the internal `_ts`/`_source` stamps ride along
as columns. The **download** writes the current SNAPSHOT under the table's declared `OutputFields`, so it
has neither the internals nor the retractions. Both are correct for what they are — a log versus a
picture — and `FileSinkConfig.Columns` trims the log's columns when the stamps are noise.

## Deliberately not done

- **XLSX.** "Similar files" is served by the delimiter sniffer; a real spreadsheet needs a new dependency
  (ClosedXML/EPPlus) and a sheet-selection UI. Say so and it can be its own wave.
- **A per-source delimiter field.** Four config classes would have grown one, for a value the header line
  already tells us. The sniffer's failure mode is loud — every row becomes one column, visible in the
  console's first schema preview — and an explicit override already exists in the parser's own signature
  if a config field ever earns its place.
- **A sink-side write path bounded to an allowed root.** The platform already lets an Editor read any
  path on the host (`file`/`folder` sources) and run arbitrary SQL; append-only writing sits inside that
  same trust envelope, and a configurable root would need plumbing through both hosts to fence a door
  whose window is already open. Documented, not pretended away.
