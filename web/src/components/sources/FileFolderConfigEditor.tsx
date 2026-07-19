import type { FileFormat, FilePollConfig, FolderPollConfig } from '@/api/types'
import { Input } from '@/components/ui/input'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { cn } from '@/lib/utils'

const FILE_FORMATS: FileFormat[] = ['ndjson', 'json', 'csv']

export interface FileFolderFormState {
  path: string
  format: FileFormat
  /** Only meaningful for the folder variant. */
  glob: string
}

export function toFileFormState(cfg?: FilePollConfig | null): FileFolderFormState {
  return { path: cfg?.path ?? '', format: cfg?.format ?? 'ndjson', glob: '' }
}

export function toFolderFormState(cfg?: FolderPollConfig | null): FileFolderFormState {
  return { path: cfg?.path ?? '', format: cfg?.format ?? 'ndjson', glob: cfg?.glob ?? '' }
}

export function buildFileConfig(state: FileFolderFormState): FilePollConfig {
  return { path: state.path.trim(), format: state.format }
}

export function buildFolderConfig(state: FileFolderFormState): FolderPollConfig {
  return { path: state.path.trim(), format: state.format, glob: state.glob.trim() || null }
}

/** file/folder-kind connector config: shared editor (folder adds a non-recursive glob). */
export function FileFolderConfigEditor({
  variant,
  value,
  onChange,
  disabled = false,
}: {
  variant: 'file' | 'folder'
  value: FileFolderFormState
  onChange: (patch: Partial<FileFolderFormState>) => void
  disabled?: boolean
}) {
  return (
    <FieldGroup className="gap-3">
      <Field>
        <FieldLabel htmlFor="ff-cfg-path">{variant === 'folder' ? 'Folder path' : 'File path'}</FieldLabel>
        <Input
          id="ff-cfg-path"
          value={value.path}
          onChange={(e) => onChange({ path: e.target.value })}
          placeholder={variant === 'folder' ? '/data/incoming' : '/data/incoming/trades.ndjson'}
          disabled={disabled}
          className="font-mono"
        />
      </Field>
      <div className={cn('grid gap-3', variant === 'folder' ? 'grid-cols-2' : 'grid-cols-1')}>
        <Field>
          <FieldLabel htmlFor="ff-cfg-format">Format</FieldLabel>
          <Select value={value.format} onValueChange={(v) => onChange({ format: v as FileFormat })} disabled={disabled}>
            <SelectTrigger id="ff-cfg-format" className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {FILE_FORMATS.map((f) => (
                  <SelectItem key={f} value={f}>
                    {f}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </Field>
        {variant === 'folder' && (
          <Field>
            <FieldLabel htmlFor="ff-cfg-glob">Glob (optional)</FieldLabel>
            <Input
              id="ff-cfg-glob"
              value={value.glob}
              onChange={(e) => onChange({ glob: e.target.value })}
              placeholder="*.json"
              disabled={disabled}
              className="font-mono"
            />
          </Field>
        )}
      </div>
      {variant === 'folder' && (
        <p className="text-[11px] text-muted-foreground">Non-recursive — matches file names directly inside the folder.</p>
      )}
    </FieldGroup>
  )
}
