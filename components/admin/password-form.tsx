"use client"

import { useActionState } from "react"

import { FormStatus } from "./form-status"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { resetPasswordAction, type AdminFormState } from "@/lib/admin/actions"

export function PasswordForm({ userId }: { userId: number }) {
  const [state, action, pending] = useActionState<AdminFormState, FormData>(
    resetPasswordAction.bind(null, userId),
    {}
  )
  return (
    <form action={action} className="flex flex-col gap-4">
      <div className="grid gap-1.5">
        <Label htmlFor="reset-password">New password</Label>
        <Input
          id="reset-password"
          name="password"
          type="password"
          autoComplete="new-password"
          minLength={8}
          required
        />
      </div>
      <FormStatus state={state} />
      <div>
        <Button type="submit" variant="outline" disabled={pending}>
          {pending ? "Saving…" : "Set password"}
        </Button>
      </div>
    </form>
  )
}
