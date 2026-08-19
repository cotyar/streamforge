import { Input } from '@/components/ui/input'
import { Field, FieldDescription, FieldLabel } from '@/components/ui/field'

/** Comma-separated in, trimmed array out.
 *
 *  ponytail: a plain text input rather than a chips/token control. Ceiling: no autocomplete against the
 *  known role or group names, and a name containing a comma cannot be entered (no name in this system
 *  may contain one anyway). Upgrade path is a token input backed by the policy document the page has
 *  already loaded — worth doing when someone complains, not before. */
export function StringListField({
  id,
  label,
  description,
  value,
  onChange,
  placeholder,
  disabled,
}: {
  id: string
  label: string
  description?: string
  value: string[]
  onChange: (next: string[]) => void
  placeholder?: string
  disabled?: boolean
}) {
  return (
    <Field>
      <FieldLabel htmlFor={id}>{label}</FieldLabel>
      <Input
        id={id}
        value={value.join(', ')}
        disabled={disabled}
        placeholder={placeholder}
        onChange={(e) => onChange(splitList(e.target.value))}
      />
      {description && <FieldDescription>{description}</FieldDescription>}
    </Field>
  )
}

export function splitList(raw: string): string[] {
  return raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
}
