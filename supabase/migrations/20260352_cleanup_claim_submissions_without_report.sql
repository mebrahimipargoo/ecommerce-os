-- Remove queued / placeholder claim_submissions that never received a stored PDF (dummy or abandoned rows).
-- claim_history_logs rows CASCADE with submission delete.
-- Note: This repo has no `claim_cases` table; if your database includes one, delete orphaned rows there separately.

DELETE FROM public.claim_submissions
WHERE report_url IS NULL
   OR btrim(report_url) = '';
