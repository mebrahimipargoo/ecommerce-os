/**
 * PostgREST table name and safe column handling for `public.claim_submissions`.
 * Always use `CLAIM_SUBMISSIONS_TABLE` in `.from()` so renames stay centralized.
 */
import { RETURNS_CLAIM_EMBED } from "../returns/returns-constants";

export const CLAIM_SUBMISSIONS_TABLE = "claim_submissions" as const;

/** FK to `returns.id` — one submission row per return item ready to file. */
export const CLAIM_SUBMISSION_RETURN_ID_COLUMN = "return_id" as const;

/** `claim_submissions` with embedded `returns` columns (no phantom `product_identifier`). */
export const CLAIM_SUBMISSIONS_WITH_RETURNS_EMBED = `*, returns(${RETURNS_CLAIM_EMBED})`;
