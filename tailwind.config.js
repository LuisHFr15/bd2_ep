module.exports = {
  purge: [],
  darkMode: false, // or 'media' or 'class'
  content: [
    "./src/templates/**/*.html",
    "./src/static/**/*.js",  
    "./node_modules/preline/dist/*.js"
  ],
  theme: {
    extend: {
      fontFamily: {
        monda: ["Monda", "sans-serif"]
      }
    },
  },
  variants: {
    extend: {},
  },
  plugins: [],
}
