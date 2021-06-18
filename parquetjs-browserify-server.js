const express = require('express')
const app = express()
const port = 8000
const bfy = require("browserify")
const fs = require("fs")
const path = require("path")

// browserify parquet.js -p [ tsify --noImplicitAny -p ./tsconfig.json] -t [ babelify --plugins [ @babel/plugin-transform-class-properties ]] --debug=true > bundle.js

app.get('/parquetjs', (req, res) => {
    const bundlePath = path.join(__dirname,"./bundle.js")
    // const bundle = fs.createWriteStream(bundlePath);
    //
    // bfy("./parquet.js")
    //     .transform("babelify", {
    //         plugins: ["@babel/plugin-transform-class-properties"]
    //     })
    //     .plugin("tsify", {
    //         noImplicityAny: true,
    //         project: "./tsconfig.json"
    //     })
    //     .bundle()
    //     .on('error', (e) => console.error(e.toString()))
    //     .pipe(bundle)
    res.sendFile(bundlePath)
})

app.listen(port, () => {

    console.log(`Example app listening at http://localhost:${port}`)
})
