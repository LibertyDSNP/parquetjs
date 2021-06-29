const path = require("path")
const {compressionBrowserPlugin, wasmPlugin} = require("./esbuild-plugins");
// esbuild has TypeScript support by default
const outfile = 'parquet-bundle.min.js'
require('esbuild')
    .build({
        bundle: true,
        entryPoints: ['parquet.js'],
        outdir: path.resolve(__dirname, "dist","browser"),
        define: {
            "process.env.NODE_DEBUG": false,
            "process.env.NODE_ENV": "\"production\"",
            global: "window"
        },
        globalName: 'parquetjs',
        inject: ['./esbuild-shims.js'],
        minify: true,
        platform: 'browser',  // default
        plugins: [compressionBrowserPlugin, wasmPlugin],
        target: "esnext"  // default
    })
    .then(res => {
        if (!res.warnings.length) {
            console.log("built with no errors or warnings")
        }
    })
    .catch(e => {
        console.error("Finished with errors: ", e.toString());
    });



