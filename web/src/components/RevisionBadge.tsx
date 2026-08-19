import { Badge } from '@/components/ui/badge'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

/** Plan 016 wave 2-B: registry-assigned Revision/SchemaRevision, for wherever an operator would look for
 *  "which version of this did I just edit" — next to the status badge on a detail page, or in the name
 *  cell on a list page.
 *
 *  Rendered ONLY when `revision` is present (`!== undefined`) — absent means a pre-016 server, or a
 *  record written before this wave, and the console must render exactly as it did before rather than
 *  show a badge for a revision nobody ever assigned. `schemaRevision` is optional independent of that
 *  (pipelines carry Revision but not SchemaRevision — nothing reads a pipeline's output by name, so it
 *  has no field shape to version) and folds into the tooltip only when it differs from `revision`, since
 *  most edits bump both together and repeating the same number twice tells the operator nothing new. */
export function RevisionBadge({
  revision,
  schemaRevision,
  className,
}: {
  revision?: number
  schemaRevision?: number
  className?: string
}) {
  if (revision === undefined) return null

  const showSchema = schemaRevision !== undefined && schemaRevision !== revision

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge variant="outline" className={cn('gap-1 font-mono text-[10px] text-muted-foreground', className)}>
            rev {revision}
          </Badge>
        </TooltipTrigger>
        <TooltipContent side="top">
          Revision {revision}
          {showSchema ? ` · schema revision ${schemaRevision}` : ''}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
