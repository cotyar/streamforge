import path from 'node:path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// SF_PROXY_TARGET=http://localhost:5399 points the dev SPA at the Dapr flavor.
const target = process.env.SF_PROXY_TARGET ?? 'http://localhost:5199'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    proxy: {
      '/api': target,
      '/hubs': { target, ws: true },
    },
  },
})
