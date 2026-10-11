import type { NextConfig } from "next"

const nextConfig: NextConfig = {
  // The WhatsApp data (1GB+ DB, media) is read at runtime; never bundle or trace it.
  outputFileTracingExcludes: { "*": ["./whatsapp-data/**"] },
}

export default nextConfig
