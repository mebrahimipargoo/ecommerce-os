/**
 * PostgREST table name and safe column handling for `public.claim_submissions`.
 * Always use `CLAIM_SUBMISSIONS_TABLE` in `.from()` so renames stay centralized.
 */
import { RETURNS_EMBED_SELECTOR } from "../returns/returns-constants";

export const CLAIM_SUBMISSIONS_TABLE = "claim_submissions" as const;

/** FK to `returns.id` — one submission row per return item ready to file. */
export const CLAIM_SUBMISSION_RETURN_ID_COLUMN = "return_id" as const;

/** `claim_submissions` with embedded `returns` (wildcard columns + store embed — avoids phantom legacy columns). */
export const CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED = `*,returns(${RETURNS_EMBED_SELECTOR})`;
