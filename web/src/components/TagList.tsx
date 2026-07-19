import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

/** Compact read-only tag badges for list rows (PipelinesPage/TablesPage/SourcesPage) — see
 * MetadataEditor for the editable form used on detail pages. */
export function TagList({ tags, className }: { tags: string[]; className?: string }) {
  if (tags.length === 0) return null
  return (
    <div className={cn('flex flex-wrap gap-1', className)}>
      {tags.map((t) => (
        <Badge key={t} variant="secondary" className="text-[10px]">
          {t}
        </Badge>
      ))}
    </div>
  )
}
