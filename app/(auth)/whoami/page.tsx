import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ViewerPicker } from "@/components/viewer/viewer-picker"
import { requireSession, withData } from "@/lib/access/dal"
import { peopleWithCounts } from "@/lib/stats/engine"
import { ownerName } from "@/lib/whatsapp/contacts"
import { DEFAULT_OWNER_LABEL } from "@/lib/whatsapp/viewer"

export const metadata = { title: "Who's viewing? · WhatsApp Stats" }

/** Shown after every sign-in: one login may be shared, so each session says who's looking. */
export default async function WhoAmIPage(props: PageProps<"/whoami">) {
  const session = await requireSession()
  const next = (await props.searchParams).next
  const { people, owner } = await withData(() => ({ people: peopleWithCounts(), owner: ownerName() }))
  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle>Who&apos;s viewing?</CardTitle>
        <CardDescription>
          Pick yourself so your messages show as “You”. This only applies to this sign-in, and you can change it anytime in
          Settings.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ViewerPicker
          people={people}
          current={session.viewerJid}
          ownerLabel={owner ? `${owner} (phone owner)` : `${DEFAULT_OWNER_LABEL}: the person whose phone this is`}
          next={typeof next === "string" ? next : "/"}
        />
      </CardContent>
    </Card>
  )
}
