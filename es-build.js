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
        define: {
            "process.env.NODE_DEBUG": false,
            "process.env.NODE_ENV": "\"development\"",
            global: "window"
        },
        // inject: ["./node_modules/browserfs/dist/shims/fs.js"],
        // inject: ["./node_modules/browserify-path/],
        platform: 'browser',
        sourcemap: "external",
        // outfile: './bundle.js',
        // plugins: [GlobalsPlugin({
        //   fs: () => {console.log("taco")}
        // })],
        bundle: true,
        globalName: 'pjs',
        inject: ['./esbuild-shims.js']

      }).then(server => {
          console.log("hi", server)
        // Call "stop" on the web server when you're done
        // server.stop()
      })
