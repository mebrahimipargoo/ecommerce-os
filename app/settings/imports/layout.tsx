import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Imports · Data Management",
  description: "Upload and track large Amazon CSV imports.",
};

export default function ImportsLayout({ children }: { children: React.ReactNode }) {
  return children;
}
