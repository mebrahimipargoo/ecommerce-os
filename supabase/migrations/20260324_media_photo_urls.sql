-- ============================================================
-- Migration: 20260324_media_photo_urls.sql
-- Purpose:   Add general photo_url column to packages and pallets
--            for box/pallet photos captured via the mobile camera
--            upload feature. Images are stored in the 'media' bucket.
-- ============================================================

-- 1. Add photo_url to packages
ALTER TABLE packages
  ADD COLUMN IF NOT EXISTS photo_url TEXT DEFAULT NULL;

COMMENT ON COLUMN packages.photo_url
  IS 'Public URL of general box/package photo stored in the media storage bucket.';

-- 2. Add photo_url to pallets
ALTER TABLE pallets
  ADD COLUMN IF NOT EXISTS photo_url TEXT DEFAULT NULL;

COMMENT ON COLUMN pallets.photo_url
  IS 'Public URL of general pallet photo stored in the media storage bucket.';

-- 3. Ensure the 'media' storage bucket exists (run once in Supabase dashboard
--    or via the Storage API if not already created).
--
--    Via SQL (Supabase Storage schema):
--    INSERT INTO storage.buckets (id, name, public)
--    VALUES ('media', 'media', true)
--    ON CONFLICT (id) DO NOTHING;
--
--    NOTE: The bucket must be set to PUBLIC so that getPublicUrl() returns
--    accessible URLs. Enable RLS policies as needed for your org.
