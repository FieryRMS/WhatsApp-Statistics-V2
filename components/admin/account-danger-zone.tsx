"use client"

import { useState, useTransition } from "react"

import { FormStatus } from "./form-status"
import { Button } from "@/components/ui/button"
import { deleteUserAction, revokeSessionsAction, type AdminFormState } from "@/lib/admin/actions"

export function AccountDangerZone({ userId, isSelf, sessions }: { userId: number; isSelf: boolean; sessions: number }) {
  const [state, setState] = useState<AdminFormState>({})
  const [pending, start] = useTransition()
  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-3">
        <Button variant="outline" disabled={pending || sessions === 0} onClick={() => start(async () => setState(await revokeSessionsAction(userId)))}>
          Sign out everywhere{sessions ? ` (${sessions})` : ""}
        </Button>
        {!isSelf && (
          <Button
            variant="destructive"
            disabled={pending}
            onClick={() => {
              if (confirm("Delete this account and its filtered data copy? This can't be undone.")) {
                start(async () => setState(await deleteUserAction(userId)))
              }
            }}
          >
            Delete account
          </Button>
        )}
      </div>
      <FormStatus state={state} />
    </div>
  )
}
