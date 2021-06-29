/**
 * Left here in case esbuild stops working for us when we try to re-enable
 * LZO and Brotli, since it took a lot of time to get the right configuration
 * Warning: this is much slower than esbuild.
 * To use, you will need to re-install webpack, webpack-cli, source-map-loader.
 * @type {path.PlatformPath | path}
 */
const path = require('path');
const webpack = require("webpack")

const BufferPlugin = new webpack.ProvidePlugin({
    process: 'process/browser',
    Buffer: ['buffer', 'Buffer'],
})

const processPlugin = new webpack.ProvidePlugin({ process: 'process/browser', })

let config = {
    target: 'web',   // should be the default
    entry: './bootstrap.js',
    output: {
        path: path.resolve(__dirname),
        filename: "bundle.js",
        library: 'parquetjs',
        wasmLoading: 'fetch', // should be the default when target is 'web'
    },
    devServer: {
        open: true,
        headers: {"Access-Control-Allow-Origin": "*"},
        host: 'localhost',
        port: 8000,
        injectClient: false   // This is what allows the module to be available to browser scripts.
    },
    devtool: "source-map",
    experiments: {
        asyncWebAssembly: true,
        // topLevelAwait: true  // maybe not needed?
    },
    plugins: [
        BufferPlugin,
        processPlugin ],
    module: {
        rules: [
            {
                test: /\.(js|ts|tsx)$/i,
                loader: 'ts-loader',
                options: {
                    logLevel: "warn",
                },
                exclude: ['/node_modules/'],
            },
            {
                test: /\.js$/,
                enforce: "pre",
                use: ["source-map-loader"],
            },
            // {
            //     test: /\.wasm$/,
            //     type: 'webassembly/sync',
            // }
            // Add your rules for custom modules here
            // Learn more about loaders from https://webpack.js.org/loaders/
        ],
    },
    node: {
        global: true,
        __filename: false,
        __dirname: false,
    },
    resolve: {
        extensions: ['.ts', '.js', '.wasm'],
        // this uses a browser version of compression.js that
        //  currently does not include LZO or Brötli comprssion
        alias: {
            "./compression": "./browser/compression"
        },
        fallback: {
            "assert": require.resolve("assert"),
            "events": require.resolve("events"),
            "fs": require.resolve("browserfs"),
            "path": require.resolve("path-browserify"),
            "stream": require.resolve("readable-stream"),
            "thrift": "./node_modules/thrift/lib/nodejs/lib/thrift/browser.js",
            "util": require.resolve("util"),
            "zlib": require.resolve("browserify-zlib"),

        } ,

    },
};

module.exports = (isProduction) => {
    if (isProduction) {
        config.mode = 'production';
    } else {
        config.mode = 'development';
    }
    return config;
};
