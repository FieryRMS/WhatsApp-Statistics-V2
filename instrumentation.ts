export async function register() {
  // Node.js only: print the one-time setup token on first boot (until an admin exists).
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { announceSetupToken } = await import("./lib/auth/setup")
    announceSetupToken()
  }
}
