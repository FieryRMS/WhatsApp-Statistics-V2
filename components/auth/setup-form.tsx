"use client"

import { useActionState } from "react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { setupAction, type FormState } from "@/lib/auth/actions"

export function SetupForm() {
  const [state, action, pending] = useActionState<FormState, FormData>(setupAction, {})
  return (
    <Card>
      <CardHeader>
        <CardTitle>Create the admin account</CardTitle>
        <CardDescription>
          The admin can see every chat and decides what other accounts can access. Copy the setup token from the
          server console. It&apos;s printed each time the server starts until setup is done.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form action={action} className="flex flex-col gap-4">
          <div className="grid gap-1.5">
            <Label htmlFor="token">Setup token</Label>
            <Input id="token" name="token" autoComplete="off" spellCheck={false} required autoFocus className="font-mono" />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="username">Admin username</Label>
            <Input id="username" name="username" autoComplete="username" defaultValue={state.username} required />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="password">Password</Label>
            <Input id="password" name="password" type="password" autoComplete="new-password" minLength={8} required />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="confirm">Confirm password</Label>
            <Input id="confirm" name="confirm" type="password" autoComplete="new-password" minLength={8} required />
          </div>
          {state.error && (
            <Alert variant="destructive">
              <AlertDescription>{state.error}</AlertDescription>
            </Alert>
          )}
          <Button type="submit" disabled={pending}>
            {pending ? "Creating…" : "Create admin account"}
          </Button>
        </form>
      </CardContent>
    </Card>
  )
}
