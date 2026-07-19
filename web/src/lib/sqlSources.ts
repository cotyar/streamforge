// Best-effort extraction of source names referenced by a streaming-SQL statement,
// used purely for display (e.g. a "sources" column in the pipelines table).
const SOURCE_REF_REGEX = /\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)/gi

export function extractSourcesFromSql(sql: string): string[] {
  const found = new Set<string>()
  let m: RegExpExecArray | null
  const re = new RegExp(SOURCE_REF_REGEX)
  while ((m = re.exec(sql))) {
    found.add(m[1])
  }
  return Array.from(found)
}
