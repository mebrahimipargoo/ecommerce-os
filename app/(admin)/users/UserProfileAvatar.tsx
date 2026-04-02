"use client";

import React, { useState } from "react";
import { Avatar, AvatarFallback, AvatarImage } from "../../../components/ui/avatar";

/** Standard circular avatar: 48×48, object-cover, initials fallback. */
export function UserProfileAvatar({
  name,
  photoUrl,
  className,
}: {
  name: string;
  photoUrl: string | null;
  className?: string;
}) {
  const [broken, setBroken] = useState(false);
  const initial = name.trim().charAt(0).toUpperCase() || "?";

  return (
    <Avatar className={className}>
      {photoUrl?.trim() && !broken ? (
        <AvatarImage
          src={photoUrl}
          alt=""
          onError={() => setBroken(true)}
        />
      ) : (
        <AvatarFallback>{initial}</AvatarFallback>
      )}
    </Avatar>
  );
}
