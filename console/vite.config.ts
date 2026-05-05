import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Multi-bundle build driven by the APP env var.
//   APP unset (default)  -> legacy single bundle (`dist/`, entry `index.html`).
//   APP=ops              -> ops bundle (`dist/ops/`, entry `ops.html`).
//   APP=tenant           -> tenant bundle (`dist/tenant/`, entry `tenant.html`).
//
// `npm run build` runs both ops and tenant builds back-to-back so the
// gateway's `OBJECTIO_OPS_CONSOLE_DIR` and `OBJECTIO_TENANT_CONSOLE_DIR`
// each have a tree to serve. Legacy single-bundle build is still
// available for `npm run build:legacy` when you need the old dist/
// layout (e.g. tooling that pre-dates the split).
const app = process.env.APP;

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: "/_console/",
  server: {
    proxy: {
      "/_admin": { target: "https://s3.imys.in", changeOrigin: true },
      "/iceberg": { target: "https://s3.imys.in", changeOrigin: true },
      "/metrics": { target: "https://s3.imys.in", changeOrigin: true },
      "/health": { target: "https://s3.imys.in", changeOrigin: true },
    },
  },
  build: app
    ? {
        // Per-bundle build. Rollup uses the input *key* as the output
        // filename, so `{ index: "ops.html" }` produces
        // `dist/ops/index.html` — exactly what tower_http::ServeDir's
        // SPA fallback expects.
        outDir: `dist/${app}`,
        assetsDir: "assets",
        emptyOutDir: true,
        rollupOptions: {
          input: { index: `${app}.html` },
        },
      }
    : {
        outDir: "dist",
        assetsDir: "assets",
      },
});
