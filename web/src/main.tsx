import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App.tsx'
import './index.css'
import { loadUiPlugins } from './plugins/load'

// UI plugins register their editors before the first render — see src/plugins/registry.tsx. Awaited, not
// fired-and-forgotten: a plugin registering after a panel has already rendered would silently not apply.
await loadUiPlugins()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
