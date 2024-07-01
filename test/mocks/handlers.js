const { http } = require('msw');
const fs = require('fs');
const fsPromises = require('fs/promises');
const util = require('util');
const path = require('path');
const readPromsify = util.promisify(fs.read);

const rangeHandle = http.get('http://fruits-bloomfilter.parquet', async ({ request }) => {
  const fd = fs.openSync(path.resolve(__dirname, '../../fruits-bloomfilter.parquet'), 'r');

  const { size: fileSize } = await fsPromises.stat(path.resolve(__dirname, '../../fruits-bloomfilter.parquet'));

  const rangeHeader = request.headers.get('range');
  if (!rangeHeader) {
    return new Response('', {
      headers: {
        'Content-Length': fileSize,
      },
    });
  }

  const [start, end] = rangeHeader
    .replace(/bytes=/, '')
    .split('-')
    .map(Number);
  const chunk = end - start + 1;

  const { bytesRead, buffer } = await readPromsify(fd, Buffer.alloc(chunk), 0, chunk, start);

  const headers = {
    'Accept-Ranges': 'bytes',
    'Content-Ranges': `bytes ${start}-${end}/${bytesRead}`,
    'Content-Type': 'application/octet-stream',
    'Content-Length': fileSize,
  };

  return new Response(buffer, {
    status: 206,
    headers,
  });
});

const handlers = [rangeHandle];

module.exports = handlers;
