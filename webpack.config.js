// Generated using webpack-cli https://github.com/webpack/webpack-cli
const path = require('path');
const webpack = require("webpack")

const BufferPlugin = new webpack.ProvidePlugin({
    process: 'process/browser',
    Buffer: ['buffer', 'Buffer'],
})

const processPlugin = new webpack.ProvidePlugin({ process: 'process/browser', })

let config = {
    target: 'web',   // should be default
    entry: './bootstrap.js',
    output: {
        path: path.resolve(__dirname),
        filename: "bundle.js",
        library: 'parquetjs',
        wasmLoading: 'fetch', // should be default when target is 'web'
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
        // topLevelAwait: true
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
        //  "browser": {
        //     "assert": "assert",
        //     "events": "events",
        //     "fs": "browserfs"
        //     "path": "path-browserify",
        //     "stream": "readable-stream",
        //     "thrift": "./node_modules/thrift/lib/nodejs/lib/thrift/browser.js",
        //     "util": "util",
        //     "zlib": "browserify-zlib",
        //   },
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
