import type { LucideIcon } from "lucide-react";
import {
  Banknote,
  Building2,
  Database,
  DollarSign,
  FileText,
  Network,
  Package,
  Palette,
  RotateCcw,
  ScanLine,
  Settings,
  Shield,
  ShieldAlert,
  Users,
  ClipboardList,
} from "lucide-react";
import type { SidebarIconName } from "./sidebar-config";

export const SIDEBAR_ICONS: Record<SidebarIconName, LucideIcon> = {
  Package: Package,
  RotateCcw: RotateCcw,
  ClipboardList: ClipboardList,
  Banknote: Banknote,
  DollarSign: DollarSign,
  ShieldAlert: ShieldAlert,
  FileText: FileText,
  Settings: Settings,
  Building2: Building2,
  Users: Users,
  Palette: Palette,
  Network: Network,
  Shield: Shield,
  Database: Database,
  ScanLine: ScanLine,
};

export function getSidebarIcon(name: SidebarIconName): LucideIcon {
  return SIDEBAR_ICONS[name] ?? Settings;
}
