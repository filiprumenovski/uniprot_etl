/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  images: {
    unoptimized: true,
  },
  // Tauri doesn't support trailing slashes
  trailingSlash: false,
};

module.exports = nextConfig;
