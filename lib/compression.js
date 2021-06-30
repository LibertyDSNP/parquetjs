'use strict';
const zlib = require('zlib');
const snappy = require('snappyjs');
import { compress as brotliCompress, decompress as brotliDecompress } from 'wasm-brotli'


// LZO compression is disabled. See: https://github.com/LibertyDSNP/parquetjs/issues/18
const PARQUET_COMPRESSION_METHODS = {
  'UNCOMPRESSED': {
    deflate: deflate_identity,
    inflate: inflate_identity
  },
  'GZIP': {
    deflate: deflate_gzip,
    inflate: inflate_gzip
  },
  'SNAPPY': {
    deflate: deflate_snappy,
    inflate: inflate_snappy
  },
  'BROTLI': {
    deflate: deflate_brotli,
    inflate: inflate_brotli
  }
};

/**
 * Deflate a value using compression method `method`
 */
async function deflate(method, value) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

function deflate_identity(value) {
  return value;
}

function deflate_gzip(value) {
  return zlib.gzipSync(value);
}

function deflate_snappy(value) {
  return snappy.compress(value);
}

async function deflate_brotli(value) {
  const compressedContent =  await brotliCompress(value, {
    mode: 0,
    quality: 8,
    lgwin: 22
  })
  return Buffer.from(compressedContent);
}

/**
 * Inflate a value using compression method `method`
 */
async function inflate(method, value) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return await PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

function inflate_identity(value) {
  return value;
}

function inflate_gzip(value) {
  return zlib.gunzipSync(value);
}

function inflate_snappy(value) {
  return snappy.uncompress(value);
}

async function inflate_brotli(value) {
  const uncompressedContent = await brotliDecompress(value)
  return Buffer.from(uncompressedContent);
}

module.exports = { PARQUET_COMPRESSION_METHODS, deflate, inflate };

