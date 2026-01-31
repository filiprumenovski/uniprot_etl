import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "UniProt ETL",
  description: "UniProt XML to Parquet ETL Pipeline",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <div className="min-h-screen bg-background">
          <header className="border-b">
            <div className="container mx-auto px-4 py-4">
              <nav className="flex items-center justify-between">
                <div className="flex items-center gap-6">
                  <h1 className="text-xl font-bold">UniProt ETL</h1>
                  <div className="flex gap-4">
                    <a href="/" className="text-sm hover:text-primary">
                      Dashboard
                    </a>
                    <a href="/runs" className="text-sm hover:text-primary">
                      Run History
                    </a>
                  </div>
                </div>
              </nav>
            </div>
          </header>
          <main className="container mx-auto px-4 py-6">{children}</main>
        </div>
      </body>
    </html>
  );
}
