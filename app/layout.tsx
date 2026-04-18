import type { Metadata } from "next";
import { Inter, Geist_Mono } from "next/font/google";
import "./globals.css";
import { ThemeProvider } from "../components/providers/ThemeProvider";
import { DebugModeProvider } from "../components/DebugModeContext";
import { AppShell } from "../components/AppShell";
import { getPlatformAppNameForMetadata } from "../lib/platform-settings-read";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
  display: "swap",
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export async function generateMetadata(): Promise<Metadata> {
  const title = await getPlatformAppNameForMetadata();
  return {
    title,
    description: "B2B Returns & Recovery Platform",
  };
}

export const viewport = {
  width: "device-width",
  initialScale: 1,
  /** Zebra / rugged Android browsers: allow pinch-zoom; fixed max scale can break layout on some WebViews. */
  maximumScale: 5,
  userScalable: true,
  viewportFit: "cover",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`${inter.variable} ${geistMono.variable} font-sans antialiased`}>
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <DebugModeProvider>
            {/*
             * AppShell renders the persistent collapsible sidebar on desktop
             * and a hamburger-triggered drawer on mobile.
             * Every page route is wrapped here — the sidebar NEVER disappears.
             */}
            <AppShell>{children}</AppShell>
          </DebugModeProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
