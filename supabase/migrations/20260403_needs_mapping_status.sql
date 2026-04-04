-- Add `needs_mapping` as a valid lifecycle status on raw_report_uploads.
-- Rows land here when header classification returns UNKNOWN or required fields are absent.
-- UI shows a "Map Columns" button; after the user saves their mapping the status flips to
-- `pending` and the normal Sync pipeline resumes.

ALTER TABLE public.raw_report_uploads DROP CONSTRAINT IF EXISTS raw_report_uploads_status_check;

ALTER TABLE public.raw_report_uploads
  ADD CONSTRAINT raw_report_uploads_status_check CHECK (
    status = ANY (ARRAY[
      'pending'::text,
      'uploading'::text,
      'processing'::text,
      'synced'::text,
      'complete'::text,
      'failed'::text,
      'cancelled'::text,
      'needs_mapping'::text
    ])
  );

COMMENT ON COLUMN public.raw_report_uploads.status IS
  'Upload lifecycle: pending | uploading | processing | synced | complete | failed | cancelled | needs_mapping.
   needs_mapping = headers could not be classified automatically; user must map columns before syncing.';

NOTIFY pgrst, 'reload schema';
