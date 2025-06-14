module.exports = {
  purge: [],
  darkMode: false, // or 'media' or 'class'
  content: [
    "./src/templates/**/*.html",
    "./src/static/**/*.js",  
    "./node_modules/preline/dist/*.js"
  ],
  theme: {
    extend: {},
  },
  variants: {
    extend: {},
  },
  plugins: [
    require('preline/plugin'),
  ],
}
