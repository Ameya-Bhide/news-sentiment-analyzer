// pages/_app.js
import '../styles/globals.css'

export default function MyApp({ Component, pageProps }) {
  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      {/* Navbar */}
      <header className="bg-blue-600 text-white shadow">
        <div className="max-w-6xl mx-auto px-4 py-3 flex justify-between items-center">
          <h1 className="text-xl font-bold">📊 News Sentiment Dashboard</h1>
          <nav className="space-x-4">
            <a href="/" className="hover:underline">
              Sentiment
            </a>
            <a href="/market" className="hover:underline">
              Market
            </a>
          </nav>
        </div>
      </header>

      {/* Page content */}
      <main className="max-w-6xl mx-auto px-4 py-6">
        <Component {...pageProps} />
      </main>

      {/* Footer */}
      <footer className="bg-gray-100 text-center py-4 mt-8 text-sm text-gray-600 border-t">
        Built with ❤️ using Next.js + Tailwind
      </footer>
    </div>
  )
}
