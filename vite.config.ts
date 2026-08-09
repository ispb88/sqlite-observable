

import { defineConfig } from "vite";

export default defineConfig(config => {
    return {
        base: config.command === "serve" ? "/absproxy/4785/" : "/",
        root: "demo",
        server: {
            host: "127.0.0.1",
            port: 4785,
        },
        optimizeDeps: {
            exclude: ["wa-sqlite"]
        }
    }
})
