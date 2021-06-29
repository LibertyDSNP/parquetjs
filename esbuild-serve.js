const {compressionBrowserPlugin, wasmPlugin} = require("./esbuild-plugins");
// esbuild has TypeScript support by default
require('esbuild')
      .serve({
        servedir: __dirname,
      }, {
        entryPoints: ['parquet.js'],
        outfile: 'main.js',
        define: {"process.env.NODE_DEBUG": false, "process.env.NODE_ENV": "\"production\"", global: "window" },
        platform: 'browser',
        plugins: [compressionBrowserPlugin,wasmPlugin],
        sourcemap: "external",
        bundle: true,
        globalName: 'parquetjs',
        inject: ['./esbuild-shims.js']
      }).then(server => {
          console.log("serving parquetjs", server)
        // Call "stop" on the web server when you're done
        // server.stop()
      })
