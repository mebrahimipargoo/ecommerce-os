-- Item-level return label for loose items (no parent package).

ALTER TABLE public.returns
  ADD COLUMN IF NOT EXISTS photo_return_label_url TEXT DEFAULT NULL;

COMMENT ON COLUMN public.returns.photo_return_label_url IS
  'Optional return-shipping label photo when the item has no linked package (loose/orphan flow).';
