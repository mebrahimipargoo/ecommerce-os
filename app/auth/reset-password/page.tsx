"use client";

import Link from "next/link";
import { FormEvent, useEffect, useState } from "react";
import { supabase } from "@/src/lib/supabase";

/**
 * Merge fragment and query auth params (same rules as GoTrue `parseParametersFromURL`:
 * hash first, then query keys overwrite).
 */
function authParamsFromLocation(): URLSearchParams {
  const merged = new URLSearchParams();
  if (typeof window === "undefined") return merged;
  const url = new URL(window.location.href);
  if (url.hash.startsWith("#")) {
    new URLSearchParams(url.hash.slice(1)).forEach((value, key) => merged.set(key, value));
  }
  url.searchParams.forEach((value, key) => merged.set(key, value));
  return merged;
}

function stripAuthParamsFromAddressBar() {
  if (typeof window === "undefined") return;
  window.history.replaceState(window.history.state ?? {}, "", window.location.pathname);
}

export default function ResetPasswordPage() {
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [authChecked, setAuthChecked] = useState(false);
  const [hasSession, setHasSession] = useState(false);
  const [linkErrorMessage, setLinkErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, session) => {
      if (cancelled) return;
      setHasSession(Boolean(session));
    });

    async function resolveSession() {
      await supabase.auth.initialize();

      if (cancelled) return;

      const params = authParamsFromLocation();
      const oauthErr = params.get("error");
      const oauthErrCode = params.get("error_code");
      const oauthErrDesc = params.get("error_description");

      if (oauthErr || oauthErrCode || oauthErrDesc) {
        let msg =
          "This reset link is invalid or has expired. Request a new link from the forgot password page.";
        if (oauthErrCode === "otp_expired") {
          msg =
            "This reset link has expired. Request a new password reset email and use the link promptly.";
        }
        setLinkErrorMessage(msg);
        setHasSession(false);
        setAuthChecked(true);
        stripAuthParamsFromAddressBar();
        return;
      }

      const code = params.get("code");
      if (code) {
        const { error: exchangeError } = await supabase.auth.exchangeCodeForSession(code);
        if (cancelled) return;
        if (exchangeError) {
          const verifierMissing =
            exchangeError.message?.toLowerCase().includes("code verifier") ?? false;
          setLinkErrorMessage(
            verifierMissing
              ? "Open this reset link in the same browser where you requested the password reset (PKCE), or request a new reset email."
              : exchangeError.message || "Could not validate this reset link.",
          );
          setHasSession(false);
          setAuthChecked(true);
          return;
        }
        stripAuthParamsFromAddressBar();
      }

      const first = await supabase.auth.getSession();
      if (cancelled) return;
      if (first.data.session) setHasSession(true);
      await new Promise((r) => setTimeout(r, 350));
      if (cancelled) return;
      const second = await supabase.auth.getSession();
      if (second.data.session) setHasSession(true);
      setAuthChecked(true);
    }

    void resolveSession();

    return () => {
      cancelled = true;
      subscription.unsubscribe();
    };
  }, []);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setErrorMessage(null);
    setSuccessMessage(null);

    if (password !== confirmPassword) {
      setErrorMessage("Passwords do not match.");
      return;
    }

    if (password.length < 6) {
      setErrorMessage("Password must be at least 6 characters.");
      return;
    }

    setIsSubmitting(true);

    try {
      const { error } = await supabase.auth.updateUser({ password });

      if (error) {
        setErrorMessage(error.message || "Could not update password. Please try again.");
        return;
      }

      setSuccessMessage("Your password has been updated. You can sign in with your new password.");
    } catch {
      setErrorMessage("Unexpected error. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <main className="flex min-h-screen items-center justify-center bg-slate-50 px-4 py-10 dark:bg-slate-950">
      <section className="w-full max-w-md rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-800 dark:bg-slate-950/70">
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900 dark:text-slate-50">Reset password</h1>
        <p className="mt-2 text-sm text-muted-foreground">Choose a new password for your account.</p>

        {!authChecked ? (
          <p className="mt-6 text-sm text-muted-foreground">Checking your reset link…</p>
        ) : linkErrorMessage || !hasSession ? (
          <p className="mt-6 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-100">
            {linkErrorMessage ??
              "This reset link is invalid or has expired. Request a new link from the forgot password page."}
          </p>
        ) : (
          <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
            <div>
              <label htmlFor="password" className="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300">
                New password
              </label>
              <input
                id="password"
                type="password"
                placeholder="••••••••"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                required
                autoComplete="new-password"
                minLength={6}
                className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-900 outline-none transition focus:border-primary focus:ring-1 focus:ring-ring dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
              />
            </div>

            <div>
              <label
                htmlFor="confirm-password"
                className="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300"
              >
                Confirm new password
              </label>
              <input
                id="confirm-password"
                type="password"
                placeholder="••••••••"
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
                required
                autoComplete="new-password"
                minLength={6}
                className="h-10 w-full rounded-lg border border-slate-200 bg-white px-3 text-sm text-slate-900 outline-none transition focus:border-primary focus:ring-1 focus:ring-ring dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
              />
            </div>

            {errorMessage && (
              <p className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700 dark:border-rose-900/60 dark:bg-rose-950/40 dark:text-rose-200">
                {errorMessage}
              </p>
            )}

            {successMessage && (
              <p className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-900/60 dark:bg-emerald-950/40 dark:text-emerald-200">
                {successMessage}
              </p>
            )}

            <button
              type="submit"
              disabled={isSubmitting}
              className="inline-flex h-10 w-full items-center justify-center rounded-lg border border-slate-200 bg-slate-900 text-sm font-medium text-slate-50 transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60 dark:border-slate-700 dark:bg-slate-100 dark:text-slate-900 dark:hover:bg-white"
            >
              {isSubmitting ? "Updating…" : "Update password"}
            </button>
          </form>
        )}

        <p className="mt-6 text-center text-sm text-muted-foreground">
          <Link href="/login" className="font-medium text-slate-900 underline-offset-4 hover:underline dark:text-slate-100">
            Back to login
          </Link>
        </p>
      </section>
    </main>
  );
}
