import zlib from 'zlib'
import snappy from 'snappyjs'
import { ZstdInit } from '@oneidentity/zstd-js';
import { compress as brotliCompress, decompress as brotliDecompress } from 'wasm-brotli'

type d_identity = (value: ArrayBuffer | Buffer | Uint8Array ) => ArrayBuffer | Buffer | Uint8Array
type d_gzip = (value: ArrayBuffer | Buffer | string ) => Buffer
type d_snappy = (value: ArrayBuffer | Buffer | Uint8Array ) => ArrayBuffer | Buffer | Uint8Array
type d_brotli = (value: Uint8Array ) => Promise<Buffer>

interface PARQUET_COMPRESSION_METHODS {
  [key:string]: {
      deflate: Function
      inflate: Function
  }
}
// LZO compression is disabled. See: https://github.com/LibertyDSNP/parquetjs/issues/18
export const PARQUET_COMPRESSION_METHODS: PARQUET_COMPRESSION_METHODS = {
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
  },
  'ZSTD': {
    deflate: deflate_zstd,
    inflate: inflate_zstd
  }
};

/**
 * Deflate a value using compression method `method`
 */
export async function deflate(method: string, value: unknown) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

function deflate_identity(value: ArrayBuffer | Buffer | Uint8Array) {
  return value;
}

function deflate_gzip(value: ArrayBuffer | Buffer | string) {
  return zlib.gzipSync(value);
}

function deflate_snappy(value: ArrayBuffer | Buffer | Uint8Array) {
  return snappy.compress(value);
}

async function deflate_brotli(value: Uint8Array) {
  const compressedContent =  await brotliCompress(value/*, {
    mode: 0,
    quality: 8,
    lgwin: 22
  }
  */)
  
  return Buffer.from(compressedContent);
}

/**
 * Inflate a value using compression method `method`
 */
export async function inflate(method: string, value: unknown) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return await PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

function inflate_identity(value: ArrayBuffer | Buffer | Uint8Array) {
  return value;
}

function inflate_gzip(value: Buffer | ArrayBuffer | string) {
  return zlib.gunzipSync(value);
}

function inflate_snappy(value: ArrayBuffer | Buffer | Uint8Array) {
  return snappy.uncompress(value);
}

async function deflate_zstd(value: Buffer | Uint8Array): Promise<typeof value> {
  if (value instanceof Uint8Array && value.constructor === Uint8Array) {
    return (await ZstdInit()).ZstdSimple.compress(value);
  } else {
    return Buffer.from(
      (await ZstdInit()).ZstdSimple.compress(
        new Uint8Array(value.buffer, value.byteOffset, value.byteLength / Uint8Array.BYTES_PER_ELEMENT)
      )
    );
  }
}

async function inflate_zstd(value: Buffer | Uint8Array): Promise<typeof value> {
  if (value instanceof Uint8Array && value.constructor === Uint8Array) {
    return (await ZstdInit()).ZstdSimple.decompress(value);
  } else {
    return Buffer.from(
      (await ZstdInit()).ZstdSimple.decompress(
        new Uint8Array(value.buffer, value.byteOffset, value.byteLength / Uint8Array.BYTES_PER_ELEMENT)
      )
    );
  }
}

async function inflate_brotli(value: Uint8Array) {
  const uncompressedContent = await brotliDecompress(value)
  return Buffer.from(uncompressedContent);
}


