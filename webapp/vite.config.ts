import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/ds_utils/',
  server: { port: 6007, host: '0.0.0.0' },
  preview: { port: 6007, host: '0.0.0.0' },
})
