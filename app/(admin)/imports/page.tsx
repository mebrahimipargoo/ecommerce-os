import { redirect } from "next/navigation";

/** Bookmark URL `/imports` → primary Imports under Data Management. */
export default function AdminImportsPage() {
  redirect("/dashboard/file-import");
}
