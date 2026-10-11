"use client"

import * as React from "react"

import { anchorKey } from "@/lib/whatsapp/anchor"
import { fetchMessages } from "@/lib/whatsapp/client"
import type { ChatAnchor, Message } from "@/lib/whatsapp/types"

export interface MessageStreamOptions {
  /** Messages per request. */
  pageSize?: number
  /** Max messages kept in memory/DOM; the far end is dropped as you scroll. */
  maxWindow?: number
}

export interface MessageStreamState {
  messages: Message[]
  hasOlder: boolean
  hasNewer: boolean
  status: "loading" | "ready" | "error"
  error: string | null
  loadingOlder: boolean
  loadingNewer: boolean
  /** Message to bring into view after a jump; null means "scroll to the bottom". */
  targetId: number | null
  /** Bumped on every jump/reset so the view knows to re-position. */
  jumpVersion: number
}

export interface MessageStream extends MessageStreamState {
  loadOlder: () => void
  loadNewer: () => void
  /** Go to a point in the chat. Reuses the loaded window when the target is already in it. */
  jumpTo: (anchor: ChatAnchor) => void
  reload: () => void
}

const initialState: MessageStreamState = {
  messages: [],
  hasOlder: false,
  hasNewer: false,
  status: "loading",
  error: null,
  loadingOlder: false,
  loadingNewer: false,
  targetId: null,
  jumpVersion: 0,
}

/** Resolve an anchor inside the current window. `undefined` = not loaded, needs a fetch. */
function resolveLocally(s: MessageStreamState, anchor: ChatAnchor): number | null | undefined {
  if (s.status !== "ready" || !s.messages.length) return undefined
  const first = s.messages[0]
  const last = s.messages[s.messages.length - 1]
  switch (anchor.type) {
    case "latest":
      return s.hasNewer ? undefined : null
    case "message":
      return s.messages.some((m) => m.id === anchor.id) ? anchor.id : undefined
    case "time": {
      const coveredBefore = anchor.at >= first.timestamp || !s.hasOlder
      const coveredAfter = anchor.at <= last.timestamp || !s.hasNewer
      if (!coveredBefore || !coveredAfter) return undefined
      return (s.messages.find((m) => m.timestamp >= anchor.at) ?? last).id
    }
  }
}

function dedupe(list: Message[]) {
  const seen = new Set<number>()
  return list.filter((m) => (seen.has(m.id) ? false : (seen.add(m.id), true)))
}

/**
 * Streams a chat's messages in pages around an anchor, never loading the whole chat.
 *
 *   const stream = useMessageStream(jid, { type: "time", at: Date.parse("2021-03-05") })
 *   stream.loadOlder() / stream.loadNewer() as the user scrolls
 */
export function useMessageStream(jid: string, anchor: ChatAnchor, options: MessageStreamOptions = {}): MessageStream {
  const pageSize = options.pageSize ?? 60
  const maxWindow = options.maxWindow ?? 400

  const [state, setState] = React.useState(initialState)
  const stateRef = React.useRef(state)
  const generation = React.useRef(0)
  const inflight = React.useRef({ older: false, newer: false })
  const lastAnchorKey = React.useRef<string | null>(null)

  const update = React.useCallback((fn: (s: MessageStreamState) => MessageStreamState) => {
    setState((prev) => {
      const next = fn(prev)
      stateRef.current = next
      return next
    })
  }, [])

  const reset = React.useCallback(
    async (a: ChatAnchor) => {
      const gen = ++generation.current
      inflight.current = { older: false, newer: false }
      update((s) => ({ ...s, status: "loading", error: null }))
      try {
        const page = await fetchMessages(jid, { anchor: a }, pageSize)
        if (gen !== generation.current) return
        update((s) => ({
          messages: page.messages,
          hasOlder: page.hasOlder,
          hasNewer: page.hasNewer,
          status: "ready",
          error: null,
          loadingOlder: false,
          loadingNewer: false,
          targetId: page.targetId,
          jumpVersion: s.jumpVersion + 1,
        }))
      } catch (e) {
        if (gen !== generation.current) return
        update((s) => ({ ...s, status: "error", error: e instanceof Error ? e.message : String(e) }))
      }
    },
    [jid, pageSize, update]
  )

  const jumpTo = React.useCallback(
    (a: ChatAnchor) => {
      lastAnchorKey.current = anchorKey(a)
      const local = resolveLocally(stateRef.current, a)
      if (local !== undefined) {
        update((s) => ({ ...s, targetId: local, jumpVersion: s.jumpVersion + 1 }))
      } else {
        void reset(a)
      }
    },
    [reset, update]
  )

  const loadOlder = React.useCallback(async () => {
    const s = stateRef.current
    if (s.status !== "ready" || !s.hasOlder || inflight.current.older || !s.messages.length) return
    const gen = generation.current
    const cursor = s.messages[0].id
    inflight.current.older = true
    update((p) => ({ ...p, loadingOlder: true }))
    try {
      const page = await fetchMessages(jid, { before: cursor }, pageSize)
      if (gen !== generation.current) return
      update((p) => {
        // The window moved while we were fetching (e.g. trimmed) — drop the stale page.
        if (p.messages[0]?.id !== cursor) return { ...p, loadingOlder: false }
        let messages = dedupe([...page.messages, ...p.messages])
        let hasNewer = p.hasNewer
        if (messages.length > maxWindow) {
          messages = messages.slice(0, maxWindow)
          hasNewer = true
        }
        return { ...p, messages, hasOlder: page.hasOlder, hasNewer, loadingOlder: false }
      })
    } catch {
      if (gen === generation.current) update((p) => ({ ...p, loadingOlder: false }))
    } finally {
      if (gen === generation.current) inflight.current.older = false
    }
  }, [jid, pageSize, maxWindow, update])

  const loadNewer = React.useCallback(async () => {
    const s = stateRef.current
    if (s.status !== "ready" || !s.hasNewer || inflight.current.newer || !s.messages.length) return
    const gen = generation.current
    const cursor = s.messages[s.messages.length - 1].id
    inflight.current.newer = true
    update((p) => ({ ...p, loadingNewer: true }))
    try {
      const page = await fetchMessages(jid, { after: cursor }, pageSize)
      if (gen !== generation.current) return
      update((p) => {
        if (p.messages[p.messages.length - 1]?.id !== cursor) return { ...p, loadingNewer: false }
        let messages = dedupe([...p.messages, ...page.messages])
        let hasOlder = p.hasOlder
        if (messages.length > maxWindow) {
          messages = messages.slice(messages.length - maxWindow)
          hasOlder = true
        }
        return { ...p, messages, hasNewer: page.hasNewer, hasOlder, loadingNewer: false }
      })
    } catch {
      if (gen === generation.current) update((p) => ({ ...p, loadingNewer: false }))
    } finally {
      if (gen === generation.current) inflight.current.newer = false
    }
  }, [jid, pageSize, maxWindow, update])

  // Follow the anchor prop (e.g. from the URL). Skips anchors we already jumped to via jumpTo().
  const key = anchorKey(anchor)
  React.useEffect(() => {
    if (lastAnchorKey.current === key && stateRef.current.status !== "error") return
    jumpTo(anchor)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, jumpTo])

  const reload = React.useCallback(() => void reset(anchor), [reset, anchor])

  return {
    ...state,
    loadOlder,
    loadNewer,
    jumpTo,
    reload,
  }
}
