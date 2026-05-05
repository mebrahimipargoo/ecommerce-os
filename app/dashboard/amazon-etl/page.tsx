import { redirect } from "next/navigation";

/** Legacy URL: consolidated under Data Management → Imports. */
export default function AmazonEtlLegacyRedirectPage() {
  redirect("/dashboard/file-import");
}
