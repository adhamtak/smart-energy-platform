import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Every /api/... request is forwarded to FastAPI, so the browser sees one origin
// (no CORS problems and the login cookie just works).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': 'http://127.0.0.1:8000' },
  },
})
