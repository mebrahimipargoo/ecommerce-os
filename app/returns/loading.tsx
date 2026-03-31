/** Route-level loading UI while the Returns segment prepares (works with React Suspense boundaries). */
export default function ReturnsLoading() {
  return (
    <div className="flex min-h-[40vh] w-full flex-1 items-center justify-center p-8">
      <div className="flex flex-col items-center gap-3">
        <div className="h-10 w-10 animate-spin rounded-full border-4 border-sky-200 border-t-sky-500" />
        <p className="text-sm text-slate-400">Loading returns…</p>
      </div>
    </div>
  );
}
