import { Alert, AlertDescription } from "@/components/ui/alert"
import type { AdminFormState } from "@/lib/admin/actions"

export function FormStatus({ state }: { state: AdminFormState }) {
  if (state.error)
    return (
      <Alert variant="destructive">
        <AlertDescription>{state.error}</AlertDescription>
      </Alert>
    )
  if (state.ok)
    return (
      <Alert>
        <AlertDescription>{state.ok}</AlertDescription>
      </Alert>
    )
  return null
}
