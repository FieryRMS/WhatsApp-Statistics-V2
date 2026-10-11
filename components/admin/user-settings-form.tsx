"use client"

import { useActionState } from "react"

import { FormStatus } from "./form-status"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { updateUserAction, type AdminFormState } from "@/lib/admin/actions"
import type { User } from "@/lib/auth/store"

export function UserSettingsForm({
  user,
  isSelf,
}: {
  user: User
  isSelf: boolean
}) {
  const [state, action, pending] = useActionState<AdminFormState, FormData>(
    updateUserAction.bind(null, user.id),
    {}
  )
  return (
    <form action={action} className="flex flex-col gap-4">
      <div className="flex items-center gap-2">
        <Switch
          id="role"
          name="role"
          value="admin"
          defaultChecked={user.role === "admin"}
          disabled={isSelf}
        />
        <Label htmlFor="role">Admin (sees everything, manages accounts)</Label>
      </div>
      {isSelf && <input type="hidden" name="role" value="admin" />}
      <div className="flex items-center gap-2">
        <Switch id="media" name="media" defaultChecked={user.mediaAccess} />
        <Label htmlFor="media">Can open photos, videos and files</Label>
      </div>
      <div className="flex items-center gap-2">
        <Switch
          id="disabled"
          name="disabled"
          defaultChecked={user.disabled}
          disabled={isSelf}
        />
        <Label htmlFor="disabled">Disabled (can&apos;t sign in)</Label>
      </div>
      <FormStatus state={state} />
      <div>
        <Button type="submit" disabled={pending}>
          {pending ? "Saving…" : "Save"}
        </Button>
      </div>
    </form>
  )
}
