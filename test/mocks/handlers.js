const { rest } = require("msw");
const fs = require("fs");
const fsPromises = require("fs/promises");
const util = require("util");
const path = require("path");
const readPromsify = util.promisify(fs.read);

const rangeHandle = rest.get(
  "http://fruits-bloomfilter.parquet",
  async (req, res, ctx) => {
    const fd = fs.openSync(
      path.resolve('/tmp', 'fruits-bloomfilter.parquet'),
      "r"
    );

    const { size: fileSize } = await fsPromises.stat(
      path.resolve('/tmp', "fruits-bloomfilter.parquet")
    );

    const rangeHeader = req.headers.get("range");
    if (!rangeHeader) return res(ctx.set("Content-Length", fileSize));

    const [start, end] = rangeHeader
      .replace(/bytes=/, "")
      .split("-")
      .map(Number);
    const chunk = end - start + 1;

    const { bytesRead, buffer } = await readPromsify(
      fd,
      Buffer.alloc(chunk),
      0,
      chunk,
      start
    );

    return res(
      ctx.status(206),
      ctx.set("Accept-Ranges", "bytes"),
      ctx.set("Content-Ranges", `bytes ${start}-${end}/${bytesRead}`),
      ctx.set("Content-Type", "application/octet-stream"),
      ctx.set("Content-Length", fileSize),
      ctx.body(buffer)
    );
  }
);

const handlers = [rangeHandle];

module.exports = handlers;
