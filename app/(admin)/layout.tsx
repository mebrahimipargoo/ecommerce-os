import React from "react";
import { AdminWorkspaceGate } from "./AdminWorkspaceGate";

export default function AdminRouteGroupLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return <AdminWorkspaceGate>{children}</AdminWorkspaceGate>;
}
