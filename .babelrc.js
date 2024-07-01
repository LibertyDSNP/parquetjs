module.exports = {
  sourceType: 'unambiguous',
  plugins: ['babel-plugin-add-module-exports'],
  presets: [
    [
      '@babel/preset-env',
      {
        loose: true,
        modules: 'auto',
        useBuiltIns: 'entry', // to ensure regeneratorRuntime is defined; see bootstrap.js
        corejs: 3, // use w/ "useBuiltIns", defaults=2, must match what is in package.json
        // "targets": "> 0.25%, not dead"
      },
    ],
    '@babel/preset-typescript',
  ],
};
