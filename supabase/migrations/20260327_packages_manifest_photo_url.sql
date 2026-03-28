-- Persist packing-slip / manifest image on the package row (alongside OCR JSON).
ALTER TABLE public.packages
  ADD COLUMN IF NOT EXISTS manifest_photo_url TEXT DEFAULT NULL;

COMMENT ON COLUMN public.packages.manifest_photo_url IS
  'Public URL of the packing slip / manifest photo in the media storage bucket.';
