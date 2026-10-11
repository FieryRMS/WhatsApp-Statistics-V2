import { NextResponse, type NextRequest } from "next/server"

/**
 * Optimistic check only: visitors without a session cookie are sent to /login
 * before anything renders. This is a convenience, not the security boundary —
 * every page, Route Handler and Server Action validates the session itself
 * (lib/access/dal.ts), as the Next.js docs require.
 */
export function proxy(req: NextRequest) {
  if (req.cookies.has("wa_session")) return NextResponse.next()
  if (req.nextUrl.pathname.startsWith("/api/")) {
    return NextResponse.json({ error: "Not signed in" }, { status: 401 })
  }
  const url = req.nextUrl.clone()
  url.pathname = "/login"
  url.search = ""
  if (req.nextUrl.pathname !== "/") url.searchParams.set("next", req.nextUrl.pathname + req.nextUrl.search)
  return NextResponse.redirect(url)
}

export const config = {
  // Everything except sign-in/setup pages and static assets.
  matcher: ["/((?!login|setup|_next/static|_next/image|favicon.ico).*)"],
}
