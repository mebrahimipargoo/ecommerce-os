"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/src/lib/supabase";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setErrorMessage(null);
    setIsSubmitting(true);

    console.log("[login] submit started");
    console.log("[login] email submitted:", email);

    try {
      const response = await supabase.auth.signInWithPassword({
        email,
        password,
      });

      console.log("[login] Supabase response:", response);
      console.log("[login] session exists:", Boolean(response.data.session));
      console.log("[login] user exists:", Boolean(response.data.user));
      console.log("[login] error message:", response.error?.message ?? null);

      if (response.error) {
        console.error("[login] Supabase auth error:", response.error);
        setErrorMessage(response.error.message || "Login failed. Please try again.");
        return;
      }

      console.log("[login] success, navigating to /");
      router.push("/");
      router.refresh();
    } catch (error) {
      console.error("[login] submit exception:", error);
      setErrorMessage("Unexpected login error. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <main className="flex min-h-screen items-center justify-center bg-slate-50 px-4 py-10 dark:bg-slate-950">
      <section className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">Login</h1>
        <p className="mt-2 text-sm text-muted-foreground">Sign in with your email and password.</p>

        <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
          <div>
            <label htmlFor="email" className="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300">
              Email
            </label>
            <input
              id="email"
              type="email"
              placeholder="you@example.com"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              required
              autoComplete="email"
              className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-900 outline-none transition focus:border-primary focus:ring-1 focus:ring-ring dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
            />
          </div>

          <div>
            <label htmlFor="password" className="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300">
              Password
            </label>
            <input
              id="password"
              type="password"
              placeholder="••••••••"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              required
              autoComplete="current-password"
              className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-900 outline-none transition focus:border-primary focus:ring-1 focus:ring-ring dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
            />
          </div>

          {errorMessage && (
            <p className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700 dark:border-rose-900/60 dark:bg-rose-950/40 dark:text-rose-200">
              {errorMessage}
            </p>
          )}

          <button
            type="submit"
            disabled={isSubmitting}
            className="inline-flex h-10 w-full items-center justify-center rounded-lg border border-slate-200 bg-slate-900 text-sm font-medium text-slate-50 transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60 dark:border-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-white"
          >
            {isSubmitting ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </section>
    </main>
  );
}
