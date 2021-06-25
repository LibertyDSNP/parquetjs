// Generated using webpack-cli https://github.com/webpack/webpack-cli
const path = require('path');
const webpack = require("webpack")

const BufferPlugin = new webpack.ProvidePlugin({
    process: 'process/browser',
    Buffer: ['buffer', 'Buffer'],
})

const processPlugin = new webpack.ProvidePlugin({ process: 'process/browser', })

let config = {
    entry: './bootstrap.js',
    output: {
        path: path.resolve(__dirname),
        filename: "bundle.js",
        library: 'parquetjs',
    },
    devServer: {
        open: true,
        host: 'localhost',
        port: 8000,
        injectClient: false   // This is what allows the module to be available to browser scripts.
    },
    devtool: "source-map",
    experiments: {
        asyncWebAssembly: true,
        topLevelAwait: true
    },
    plugins: [
        BufferPlugin,
        processPlugin ],
    module: {
        rules: [
            {
                test: /\.(ts|tsx)$/i,
                loader: 'ts-loader',
                exclude: ['/node_modules/'],
            },
            {
                test: /\.js$/,
                enforce: "pre",
                use: ["source-map-loader"],
            },
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
        extensions: ['.tsx', '.ts', '.js', '.wasm'],
        fallback: {
            "stream": require.resolve("stream-browserify"),
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
