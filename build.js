require('esbuild')
    // .buildSync({
    //     entryPoints: ['parquet.js'],
    //     define: {"process.env.NODE_DEBUG": false, "process.env.NODE_ENV": "production" },
    //     platform: 'browser',
    //     bundle: true,
    //     outfile: 'out.js',
    //   })
      .serve({
        servedir: 'www',
      }, {
        entryPoints: ['parquet.js'],
        define: {"process.env.NODE_DEBUG": false, "process.env.NODE_ENV": "\"production\"", global: "window" },
        // inject: ["./node_modules/browserfs/dist/shims/fs.js"],
        // inject: ["./node_modules/browserify-path/],
        platform: 'browser',
        sourcemap: "external",
        // outfile: 'out.js',
        // plugins: [GlobalsPlugin({
        //   fs: () => {console.log("taco")}
        // })],
        bundle: true,
        globalName: 'xyz',
        outdir: 'www/js',
        inject: ['./process-shim.js']
      }).then(server => {
          console.log("hi", server)
        // Call "stop" on the web server when you're done
        // server.stop()
      })
