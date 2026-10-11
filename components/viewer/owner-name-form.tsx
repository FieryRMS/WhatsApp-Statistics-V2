"use client"

import * as React from "react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { setOwnerNameAction } from "@/lib/viewer/actions"

export function OwnerNameForm({ initial }: { initial: string }) {
  const [name, setName] = React.useState(initial)
  const [state, setState] = React.useState<{ error?: string; ok?: string }>({})
  const [pending, start] = React.useTransition()
  return (
    <form
      className="flex flex-col gap-2"
      onSubmit={(e) => {
        e.preventDefault()
        start(async () => setState(await setOwnerNameAction(name)))
      }}
    >
      <div className="flex gap-2">
        <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Raiyan" maxLength={100} />
        <Button type="submit" disabled={pending || name === initial}>
          {pending ? "Saving…" : "Save"}
        </Button>
      </div>
      {state.error && <p className="text-sm text-destructive">{state.error}</p>}
      {state.ok && <p className="text-sm text-muted-foreground">{state.ok}</p>}
    </form>
  )
}
