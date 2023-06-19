'use strict';
const zlib = require('zlib');
const snappy = require('snappyjs');

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
    if (Buffer.isBuffer(value)) {
        return value;
    } else {
        return Buffer.from(value);
    }
}

function deflate_gzip(value) {
    return zlib.gzipSync(value);
}

function deflate_snappy(value) {
    const compressedValue = snappy.compress(value);
    if (Buffer.isBuffer(compressedValue)) {
        return compressedValue;
    } else {
        return Buffer.from(compressedValue);
    }
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
    if (Buffer.isBuffer(value)) {
        return value;
    } else {
        return Buffer.from(value);
    }
}

function inflate_gzip(value) {
    return zlib.gunzipSync(value);
}

function inflate_snappy(value) {
    const uncompressedValue = snappy.uncompress(value);
    if (Buffer.isBuffer(uncompressedValue)) {
        return uncompressedValue;
      } else {
        return Buffer.from(uncompressedValue);
      }
}

exports.PARQUET_COMPRESSION_METHODS = PARQUET_COMPRESSION_METHODS
exports.deflate = deflate
exports.inflate = inflate
