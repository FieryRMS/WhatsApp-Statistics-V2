/**
 * Default hidden words for word statistics and clouds. Client-safe.
 * Each account starts with this list and can edit it (lib/stats/hidden-words.ts).
 */

const ENGLISH = `a about above after again all also am an and any are as at be because been before being below between both
but by can could did do does doing done down during each else even ever few for from further get got had has have having he her
here hers him his how i if in into is it its itself just let like me might more most much must my no nor not now of off on once
only or other our ours out over own same shall she should so some still such than that the their theirs them then there these
they this those through to too under until up upon us very was we were what when where which while who whom why will with would
yet you your yours yourself im i'm ive i've dont don't didnt didn't doesnt doesn't isnt isn't wasnt wasn't cant can't wont won't
youre you're its it's thats that's theres there's u ur ok okay oh yeah yes ya yea hmm lol`

// Common Banglish (romanised Bangla) filler.
const BANGLISH = `ami tumi tui apni amra tomra tora ora o se she e ei oi ki keno kemne kemon kothay kokhon koto kar kake
na nai nah naa ar aar abar o to toh tow re ra je jodi kintu tai tahole hoy hoi hobe hoise hoyeche hocche hoche hoilo chilo chhilo
ase ache achhe asi achi korbo korsi korchi korte kore korse korlam kor koro koren bolo boli bolse dekho dekhi jabo jai jao jaa
eta oita sheta ekta kichu sob shob onek beshi kom r k ki na vai bhai bro er ta te ke ra gula gulo gulai theke naki eita
bole jonno amar amr tomar tmr tor apnar amake tomake take hae ha haa ekhon ekhane okhane mane shudhu sudhu tahle tho dise dilo
niye keu keo accha acha achha akta kori korle dibo dite diye dise gese geche koi`

// Common Bangla (script) filler.
const BANGLA = `আমি তুমি তুই আপনি আমরা সে এ ও এই ওই কি কী কেন কেমন না নাই আর তো যে যদি কিন্তু তাই হয় হবে হয়েছে ছিল আছে করে
করবো করছি একটা কিছু সব অনেক ভাই এর টা কে থেকে জন্য আমার তোমার এখন মানে`

export const DEFAULT_HIDDEN_WORDS: string[] = [
  ...new Set(
    `${ENGLISH} ${BANGLISH} ${BANGLA}`
      .split(/\s+/)
      .map((w) => w.trim().toLowerCase())
      .filter(Boolean)
  ),
].sort((a, b) => a.localeCompare(b))

/** Same tokenisation the server uses, so hidden words match what's counted. */
export function normalizeWord(w: string): string {
  return w
    .trim()
    .toLowerCase()
    .replace(/^'+|'+$/g, "")
}
