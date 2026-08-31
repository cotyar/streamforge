# Plan 001 — shadcn/ui migration + corporate branding for the StreamsForge console

**Written against commit:** `49d6979` (repo: `/Users/yuriyhabarov/work/crates-foundation`)
**Scope root:** `/Users/yuriyhabarov/work/crates-foundation/orleans/web` — nothing outside it may be modified.
**Status:** TODO

## Why

The StreamsForge web console (React 19 + Vite 8 + Tailwind v4 SPA) was built with hand-rolled Tailwind panels and an ad-hoc sky/violet dark palette. It works, but: inconsistent spacing/focus states, custom markup where battle-tested primitives exist (tables, dialogs, badges, empty states, toasts), and no corporate identity. Goal: migrate the UI to shadcn/ui components and re-theme it with official corporate branding, without changing ANY behavior, API call, or realtime wiring.

## Design source of truth

Read BEFORE coding:
1. `/Users/yuriyhabarov/work/crates-foundation/orleans/design-system/streamsforge/MASTER.md` — persisted design system. The **Colors table there is the authoritative corporate green ramp** (Spanish Viridian `#008755` primary, Bangladesh Green `#007348`, Medium Sea Green `#39A87B`, Green Sheen `#6ABB97`, Eton Blue `#8BC8AA`, near-black green-tinted background `#0A0F0D`, panel `#10161A`). Typography: **IBM Plex Sans** (headings+body), use **IBM Plex Mono** for numeric/tabular/SQL content. Dark theme ONLY. Subtle motion (150–300 ms transitions; no decorative animation).
2. shadcn skill rules (read these files): `/Users/yuriyhabarov/.claude/skills/shadcn/rules/styling.md`, `composition.md`, `forms.md`, `icons.md`. Follow the Critical Rules exactly (semantic tokens only, `gap-*` not `space-y-*`, `size-*`, `cn()`, full Card composition, `Empty` for empty states, `Skeleton` for loading, `Badge` for status, DialogTitle always present, icons via `data-icon` in buttons).
3. Invoke the `shadcn-ui` skill (Skill tool) once before component work for integration guidance, and the `ui-ux-pro-max` skill reference `/Users/yuriyhabarov/.claude/skills/ui-ux-pro-max/references/quick-reference.md` §1 (accessibility) + §6 (typography/color) for contrast checks.

## Branding rules (hard)

- Product wordmark: **StreamsForge** with a "CORPORATE" text label in the sidebar header (stacked: small tracking-wide uppercase "CORPORATE" above the StreamsForge wordmark) and on the login card. Text only — **do NOT reproduce or approximate the client logo graphic**.
- Primary/interactive color = brand green `#008755`; hover/active `#007348`; success/"Running" uses the brand green; the old sky/violet accents are removed everywhere (including the login gradient, chart stroke, sparklines, SQL keyword color — chart/spark strokes become `#39A87B`/`#6ABB97` tints).
- Status colors: Running = brand green, Stopped = neutral gray, Failed = `#E4574D`, warnings `#E6A93C`. Keep ≥4.5:1 contrast for text on `#0A0F0D`/`#10161A` (the MASTER.md foreground/muted values are pre-checked; don't darken them).

## Package manager (hard rule)

**bun only.** `bun install`, `bun run build`, `bunx --bun shadcn@latest <cmd>`. Never npm/npx. Do not delete `package-lock.json` (harmless), bun will create `bun.lock`.

## Current state (verified at HEAD)

- `web/src/index.css` — Tailwind v4 via `@import 'tailwindcss'`; defines `--sf-*` CSS vars + `sf-flash` keyframes (row flash animation used by ResultsTable).
- `web/vite.config.ts` — react + tailwindcss plugins, `/api` + `/hubs` proxy to :5199. No path aliases (shadcn needs `@/` alias → add to both `vite.config.ts` and `tsconfig.json`).
- `web/tsconfig.json` — strict, bundler resolution, `include: ["src"]`, TypeScript 7. No `paths` yet.
- Components (all in `web/src/components/`): `Layout.tsx` (sidebar shell + RequireAuth), `Topbar.tsx`, `SqlEditor.tsx` (textarea-over-`<pre>` overlay highlighter — KEEP the mechanism, restyle colors/borders only), `PipelineBuilder.tsx`, `ResultsTable.tsx`, `MetricsBar.tsx`, `LiveChart.tsx` (hand-rolled SVG — keep, re-color), `Sparkline.tsx`, `StatusBadge.tsx`, `RoleGate.tsx`, `EmptyState.tsx`, `Skeleton.tsx`, `icons.tsx`.
- Pages in `web/src/pages/`: Login, Dashboard, Pipelines, PipelineDetail, Sources, Users. Modals are hand-rolled fixed-overlay divs in SourcesPage/UsersPage.
- Do-not-touch (behavioral seam): `web/src/api/**` (types.ts is a frozen backend contract; client/auth logic), `web/src/realtime/hub.ts`, `web/src/hooks/**`, `web/src/builder/sqlgen.ts` (pure SQL generator; only its consuming UI may change).

## Steps

1. **Baseline**: `cd orleans/web && bun install && bun run build` — must pass before any change.
2. **Alias**: add `@` → `./src` alias in `vite.config.ts` (`resolve.alias`) and `tsconfig.json` (`baseUrl`+`paths`). `bun run build` still green.
3. **shadcn init**: `bunx --bun shadcn@latest init` (defaults; base color slate — will be overridden). Expect `components.json`, `src/lib/utils.ts` (cn), and theme blocks appended to `src/index.css`. If init asks for framework detection and fails, re-run with explicit flags per `bunx --bun shadcn@latest init --help`. Verify `components.json` has `"tailwind": {"css": "src/index.css"}` and the `@/` aliases.
4. **Theme**: in `src/index.css` replace the init-generated `:root`/`.dark` variable values with the corporate ramp from MASTER.md, expressed in the format init generated (oklch or hsl — convert the hex values; keep variable NAMES exactly as shadcn expects: `--background`, `--foreground`, `--card`, `--popover`, `--primary`, `--primary-foreground`, `--secondary`, `--muted`, `--muted-foreground`, `--accent`, `--destructive`, `--border`, `--input`, `--ring`, `--chart-1..5`, `--sidebar-*`). Dark-only app: put the dark values on `:root` itself (html already has `class="dark"` in index.html — keep). Add IBM Plex Sans + IBM Plex Mono via Google Fonts `@import` at the top and set `--font-sans`/`--font-mono` in `@theme inline`. Keep the `sf-flash` keyframes (retint to brand green rgba). Delete the old `--sf-*` vars only AFTER step 6 removes their last usage; grep to confirm.
5. **Add components**: `bunx --bun shadcn@latest add button card badge table tabs dialog alert-dialog select input textarea label switch separator skeleton tooltip sonner empty spinner field input-group toggle-group dropdown-menu sheet scroll-area alert` (one call; if a name is unknown in the registry, drop just that name and note it). Then READ each added file under `src/components/ui/` — verify imports resolve and no icon-library mismatch (project uses inline SVGs in `icons.tsx`; shadcn components import `lucide-react` — `bun add lucide-react` is permitted as shadcn's icon dependency; prefer lucide icons in NEW/refactored chrome and retire `icons.tsx` if all usages migrate).
6. **Migrate UI (behavior-frozen refactor)**, file by file; after each page `bun run build` must stay green:
   - `Layout.tsx`: keep RequireAuth logic identical; rebuild sidebar with semantic tokens (`bg-sidebar`, `border-sidebar-border`), wordmark block (branding rules above), nav items as Buttons `variant="ghost"` with lucide icons, user chip with `Avatar`+`AvatarFallback`, role shown via `Badge`.
   - `LoginPage`: `Card` composition (CardHeader/CardTitle/CardDescription/CardContent), `Field`+`FieldLabel`+`Input`, brand-green primary Button, error via `Alert variant="destructive"`, demo credentials in `CardFooter` as muted mono text. Remove violet gradient; flat brand identity.
   - `DashboardPage`: stat tiles as `Card` (keep grid), pipeline cards as full `Card` composition with `Badge` status + existing Sparkline (retinted), Start/Stop as small Buttons (Editor-gated exactly as now).
   - `PipelinesPage`: shadcn `Table` composition; delete confirm becomes `AlertDialog` (was inline confirm); "New pipeline" primary Button; empty state via `Empty`.
   - `PipelineDetailPage`: SQL/Builder toggle → `Tabs` (TabsList/TabsTrigger/TabsContent); name/description → `Field`+`Input`; validation panel → `Alert` (destructive for errors, default styled success with brand-green icon for valid + plan summary); Save/Save&Start/Delete → Button variants (Delete = `variant="destructive"` + AlertDialog confirm); results/metrics/chart cards → `Card`. KEEP: debounce logic, hub subscriptions, ResultsTable flash behavior, LiveChart SVG (restroke `#39A87B`, area fill brand green at low opacity), column dropdown → `Select`.
   - `SourcesPage`: source cards → `Card`; schema table → `Table`; enable toggle → `Switch` (keep full-object PUT semantics from `sourcesApi.update` — do not change the API call shape); edit/create modal → `Dialog` (DialogTitle required) with `FieldGroup`/`Field` form rows; live tape stays, restyled mono with `ScrollArea`.
   - `UsersPage`: `Table` + `Dialog` forms + `AlertDialog` delete confirm (self-delete still blocked); role select → `Select` inside `SelectGroup`.
   - `SqlEditor.tsx`: keep overlay mechanism + diagnostics; retint token colors (keywords → `#39A87B`, strings → amber, numbers → `#6ABB97`, comments muted); container uses `border-input`/`focus-within:ring-ring` semantics; mono font var.
   - `StatusBadge.tsx` → thin wrapper over `Badge` with the status color mapping (or inline `Badge` at call sites and delete the file). `EmptyState.tsx`/`Skeleton.tsx` → replace usages with shadcn `Empty`/`Skeleton`, delete the custom files.
   - Toasts: where pages currently swallow errors silently or use inline banners for transient failures (start/stop/save errors), surface via `sonner` `toast.error(...)` — add `<Toaster />` once in Layout. Keep inline `Alert`s for form-validation errors.
7. **Cleanup**: grep for `--sf-`, `text-sky-`, `text-violet-`, `bg-sky-`, `#38bdf8`, `#a78bfa` — zero hits in `src/` outside comments; remove dead custom components; `bun run build` green with no unused-var errors (tsc strict).
8. **Final gate**: `bun run build` clean, bundle produced. Report bundle-size delta vs baseline.

## Out of scope / do NOT

- No changes outside `orleans/web/`. No backend, no `api/types.ts`, no `realtime/hub.ts` logic, no `builder/sqlgen.ts` logic, no test additions, no router restructuring, no new state libraries, no light mode, no `npm`/`npx` invocations.
- Do not alter fetch shapes, endpoint paths, role-gating conditions, subscription ref-counting, or the debounced validate interval.
- If `shadcn init` cannot detect the Vite+TS7 project or generated CSS conflicts irreconcilably with Tailwind v4.3: STOP and report back rather than hand-rolling a partial component copy.

## Done criteria (machine-checkable)

1. `cd /Users/yuriyhabarov/work/crates-foundation/orleans/web && bun run build` → exit 0.
2. `grep -rn "var(--sf-" src/ | wc -l` → 0; `grep -rn "sky-4\|violet-4\|#38bdf8\|#a78bfa" src/ | wc -l` → 0.
3. `ls src/components/ui/ | wc -l` ≥ 15 (shadcn components present).
4. `grep -rln "CORPORATE" src/ | wc -l` ≥ 2 (sidebar + login).
5. `git -C /Users/yuriyhabarov/work/crates-foundation diff --name-only` contains ONLY paths under `orleans/web/` (plus `orleans/design-system/` which pre-exists untracked).
6. Behavior freeze: `grep -n "subscribePipeline\|subscribeMetrics\|subscribeSource" src/realtime/hub.ts` unchanged vs HEAD (`git diff -- src/realtime/hub.ts` empty), same for `src/api/`.

## Maintenance note

Future component adds must go through `bunx --bun shadcn@latest add` and follow `/Users/yuriyhabarov/.claude/skills/shadcn/rules/*.md`. Brand tokens live only in `src/index.css` — never hex literals in components (the SVG chart/spark strokes and SQL token colors are the sanctioned exceptions, sourced from the MASTER.md tint ramp).
