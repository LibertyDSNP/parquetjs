'use strict';
const zlib = require('zlib');
const snappy = require('snappyjs');
// const lzo = require('lzo');
const brotli = require('wasm-brotli');

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
  'LZO': {
    deflate: deflate_lzo,
    inflate: inflate_lzo
  },
  'BROTLI': {
    deflate: deflate_brotli,
    inflate: inflate_brotli
  }
};

/**
 * Deflate a value using compression method `method`
 */
function deflate(method, value) {
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

function deflate_lzo(value) {
  return lzo.compress(value);
}

function deflate_brotli(value) {
  if (!brotli || brotli.compress === undefined) {
    throw new Error("brotli deflate is unsupported on this platform")
  }
  return Buffer.from(brotli.compress(value, {
    mode: 0,
    quality: 8,
    lgwin: 22
  }));
}

/**
 * Inflate a value using compression method `method`
 */
function inflate(method, value) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].inflate(value);
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

function inflate_lzo(value) {
  return lzo.decompress(value);
}

function inflate_brotli(value) {
  if (!brotli || brotli.decompress === undefined) {
    throw new Error("brotli.decompress is not supported on this platform")
  }
  return Buffer.from(brotli.decompress(value));
}

module.exports = { PARQUET_COMPRESSION_METHODS, deflate, inflate };

