/**
 * Layout helpers for route pages inside {@link AppShell}'s main scroller.
 * Use these so content uses horizontal space on desktop instead of staying phone-width.
 */

/** Full-bleed area: safe horizontal padding on all breakpoints. */
export const responsivePageOuter =
  "w-full min-w-0 px-4 py-6 sm:px-6 lg:px-8 lg:py-8 pb-10";

/**
 * Centered column that grows on large screens (not capped at ~max-w-xl).
 * Pair with {@link responsivePageOuter}.
 */
export const responsivePageInner =
  "mx-auto w-full max-w-5xl xl:max-w-6xl";

/** Loading / compact error states — keep readable but not full-bleed forms. */
export const responsivePageNarrow =
  "mx-auto w-full max-w-md px-4 py-12 sm:px-6";

/** Shared form control ring (matches shadcn-style inputs). */
export const responsiveFormInput =
  "h-10 w-full rounded-lg border border-input bg-background px-3 text-sm text-foreground shadow-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";

/**
 * Native `<select>` — same chrome as {@link responsiveFormInput}, with a slightly taller
 * min height on small screens for touch targets; matches `h-10` from `sm` up.
 */
export const responsiveFormSelect =
  "min-h-11 w-full rounded-lg border border-input bg-background px-3 py-2 text-sm text-foreground shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 sm:h-10 sm:min-h-0 sm:py-0";
