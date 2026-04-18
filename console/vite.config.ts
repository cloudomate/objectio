import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

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
  build: {
    outDir: "dist",
    assetsDir: "assets",
  },
});
