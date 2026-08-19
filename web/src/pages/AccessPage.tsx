import { Topbar } from '../components/Topbar'

// SEAM — plan 015 wave 6. Route, nav entry and gate exist so the three SPA agents never block each
// other on compilation; the page itself is the wave's work.
export function AccessPage() {
  return (
    <>
      <Topbar title="Access" />
      <div className="p-6 text-sm text-muted-foreground">Not built yet.</div>
    </>
  )
}
