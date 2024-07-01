const esbuild = require('esbuild');
const path = require('path');
const { compressionBrowserPlugin, wasmPlugin } = require('./esbuild-plugins');
// esbuild has TypeScript support by default
const baseConfig = {
  bundle: true,
  entryPoints: ['parquet.ts'],
  define: {
    'process.env.NODE_DEBUG': 'false',
    'process.env.NODE_ENV': '"production"',
    global: 'window',
  },
  inject: ['./esbuild-shims.js'],
  minify: true,
  mainFields: ['browser', 'module', 'main'],
  platform: 'browser', // default
  plugins: [compressionBrowserPlugin, wasmPlugin],
  target: 'es2020', // default
};
// configuration for generating test code in browser
const testConfig = {
  bundle: true,
  entryPoints: ['test/browser/main.ts'],
  define: {
    'process.env.NODE_DEBUG': 'false',
    'process.env.NODE_ENV': '"production"',
    global: 'window',
  },
  inject: ['./esbuild-shims.js'],
  minify: false,
  mainFields: ['browser', 'module', 'main'],
  platform: 'browser', // default
  plugins: [compressionBrowserPlugin, wasmPlugin],
  target: 'es2020', // default
};
const targets = [
  {
    ...baseConfig,
    globalName: 'parquetjs',
    outdir: path.resolve(__dirname, 'dist', 'browser'),
  },
  {
    ...baseConfig,
    format: 'esm',
    outfile: path.resolve(__dirname, 'dist', 'browser', 'parquet.esm.js'),
  },
  {
    ...baseConfig,
    format: 'cjs',
    outfile: path.resolve(__dirname, 'dist', 'browser', 'parquet.cjs.js'),
  },
  // Browser test code below
  {
    ...testConfig,
    outfile: path.resolve(__dirname, 'test', 'browser', 'main.js'),
  },
];
Promise.all(targets.map(esbuild.build))
  .then((results) => {
    if (results.reduce((m, r) => m && !r.warnings.length, true)) {
      console.log('built with no errors or warnings');
    }
  })
  .catch((e) => {
    console.error('Finished with errors: ', e.toString());
    process.exit(1);
  });
