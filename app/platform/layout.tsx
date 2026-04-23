import { PlatformWorkspaceGate } from "@/components/PlatformWorkspaceGate";

export default function PlatformLayout({ children }: { children: React.ReactNode }) {
  return <PlatformWorkspaceGate>{children}</PlatformWorkspaceGate>;
}
