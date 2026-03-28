/**
 * Client-side: load remote evidence URLs into data URIs for @react-pdf Image (embedded bytes, not links).
 */
export async function fetchImageAsDataUri(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, { mode: "cors", credentials: "omit" });
    if (!res.ok) return null;
    const blob = await res.blob();
    return await new Promise((resolve, reject) => {
      const fr = new FileReader();
      fr.onload = () => resolve(typeof fr.result === "string" ? fr.result : null);
      fr.onerror = () => reject(new Error("read"));
      fr.readAsDataURL(blob);
    });
  } catch {
    return null;
  }
}

export async function fetchEvidenceImagesForPdf(
  slots: { label: string; url: string }[],
): Promise<{ label: string; dataUri: string }[]> {
  const out: { label: string; dataUri: string }[] = [];
  for (const s of slots) {
    const dataUri = await fetchImageAsDataUri(s.url);
    if (dataUri) out.push({ label: s.label, dataUri });
  }
  return out;
}
