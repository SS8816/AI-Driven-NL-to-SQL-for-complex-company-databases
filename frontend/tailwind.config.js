/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class', // Enable class-based dark mode
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Dark theme colors
        dark: {
          bg: '#0B1120',
          sidebar: '#1A1F2E',
          card: '#242936',
          border: '#2D3548',
          hover: '#2A3142',
        },
        // Light theme colors (beige/off-white)
        light: {
          bg: '#FAF8F3',          // Warm off-white
          sidebar: '#F5F1E8',      // Beige sidebar
          card: '#FFFFFF',         // Pure white cards
          border: '#E6DFD0',       // Soft beige border
          hover: '#F0EBE1',        // Light beige hover
        },
        primary: {
          50: '#E6F0FF',
          100: '#CCE0FF',
          200: '#99C2FF',
          300: '#66A3FF',
          400: '#3385FF',
          500: '#0066FF',
          600: '#0052CC',
          700: '#003D99',
          800: '#002966',
          900: '#001433',
        },
        success: '#00C853',
        warning: '#FFB300',
        error: '#FF3D00',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      },
    },
  },
  plugins: [],
}
