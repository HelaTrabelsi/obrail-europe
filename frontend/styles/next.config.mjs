/** @type {import('next').NextConfig} */
const nextConfig = {
  // Mode standalone pour Docker — inclut toutes les dependances
  output: 'standalone',

  // URL de l'API FastAPI (backend)
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  },

  // Permettre les requetes vers l'API FastAPI
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: `${process.env.NEXT_PUBLIC_API_URL || 'http://api:8000'}/:path*`,
      },
    ]
  },
}

module.exports = nextConfig