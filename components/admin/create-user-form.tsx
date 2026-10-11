"use client"

import { useActionState } from "react"

import { FormStatus } from "./form-status"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { createUserAction, type AdminFormState } from "@/lib/admin/actions"

export function CreateUserForm() {
  const [state, action, pending] = useActionState<AdminFormState, FormData>(
    createUserAction,
    {}
  )
  return (
    <form action={action} className="grid gap-4 sm:grid-cols-2">
      <div className="grid gap-1.5">
        <Label htmlFor="new-username">Username</Label>
        <Input id="new-username" name="username" autoComplete="off" required />
      </div>
      <div className="grid gap-1.5">
        <Label htmlFor="new-password">Initial password</Label>
        <Input
          id="new-password"
          name="password"
          type="password"
          autoComplete="new-password"
          minLength={8}
          required
        />
      </div>
      <div className="flex items-center gap-2">
        <Switch id="new-admin" name="role" value="admin" />
        <Label htmlFor="new-admin">Admin (sees everything)</Label>
      </div>
      <div className="flex items-center gap-2">
        <Switch id="new-media" name="media" />
        <Label htmlFor="new-media">Can open photos, videos and files</Label>
      </div>
      <div className="sm:col-span-2">
        <FormStatus state={state} />
      </div>
      <div className="sm:col-span-2">
        <Button type="submit" disabled={pending}>
          {pending ? "Creating…" : "Create account"}
        </Button>
      </div>
    </form>
  )
}
