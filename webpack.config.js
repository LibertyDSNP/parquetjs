// Generated using webpack-cli https://github.com/webpack/webpack-cli
const path = require('path');
const webpack = require("webpack")
const isProduction = process.env.NODE_ENV == 'production';

// Learn more about plugins from https://webpack.js.org/configuration/plugins/
const envPlugin = new webpack.EnvironmentPlugin({
    NODE_ENV: 'development', // use 'development' unless process.env.NODE_ENV is defined
    DEBUG: false,
});

const BufferPlugin = new webpack.ProvidePlugin({
    process: 'process/browser',
    Buffer: ['buffer', 'Buffer'],
})

const processPlugin = new webpack.ProvidePlugin({ process: 'process/browser', })

const config = {
    entry: './bootstrap.js',
    output: {
        filename: 'bundle.js',
        path: path.resolve(__dirname),
    },
    devServer: {
        open: true,
        host: 'localhost',
        port: 8000,
    },
    experiments: {
        asyncWebAssembly: true,
        topLevelAwait: true
    },
    plugins: [ BufferPlugin, envPlugin, processPlugin ],
    module: {
        rules: [
            {
                test: /\.(ts|tsx)$/i,
                loader: 'ts-loader',
                exclude: ['/node_modules/'],
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

module.exports = () => {
    if (isProduction) {
        config.mode = 'production';
    } else {
        config.mode = 'development';
    }
    return config;
};
