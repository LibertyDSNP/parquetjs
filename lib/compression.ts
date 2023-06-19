import zlib from 'zlib'
import snappy from 'snappyjs'
import { compress as brotliCompress, decompress as brotliDecompress } from 'wasm-brotli'

type d_identity = (value: ArrayBuffer | Buffer | Uint8Array ) => ArrayBuffer | Buffer | Uint8Array
type d_gzip = (value: ArrayBuffer | Buffer | string ) => Buffer
type d_snappy = (value: ArrayBuffer | Buffer | Uint8Array ) => ArrayBuffer | Buffer | Uint8Array
type d_brotli = (value: Uint8Array ) => Promise<Buffer>

interface PARQUET_COMPRESSION_METHODS {
  [key:string]: {
    deflate: (value: any) => Buffer | Promise<Buffer>
    inflate: (value: any) => Buffer | Promise<Buffer>
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
  }
};

/**
 * Deflate a value using compression method `method`
 */
export async function deflate(method: string, value: unknown): Promise<Buffer> {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

function deflate_identity(value: ArrayBuffer | Buffer | Uint8Array) {
  if (Buffer.isBuffer(value)) {
    return value;
  } else {
    return Buffer.from(value);
  }
}

function deflate_gzip(value: ArrayBuffer | Buffer | string) {
  return zlib.gzipSync(value);
}

function deflate_snappy(value: ArrayBuffer | Buffer | Uint8Array) {
  const compressedValue = snappy.compress(value);
  if (Buffer.isBuffer(compressedValue)) {
    return compressedValue;
  } else {
    return Buffer.from(compressedValue);
  }
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
export async function inflate(method: string, value: unknown): Promise<Buffer> {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return await PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

async function inflate_identity(value: ArrayBuffer | Buffer | Uint8Array): Promise<Buffer> {
  if (Buffer.isBuffer(value)) {
    return value;
  } else {
    return Buffer.from(value);
  }
}

async function inflate_gzip(value: Buffer | ArrayBuffer | string) {
  return zlib.gunzipSync(value);
}

function inflate_snappy(value: ArrayBuffer | Buffer | Uint8Array) {
  const uncompressedValue = snappy.uncompress(value);
  if (Buffer.isBuffer(uncompressedValue)) {
    return uncompressedValue;
  } else {
    return Buffer.from(uncompressedValue);
  }
}

async function inflate_brotli(value: Uint8Array) {
  const uncompressedContent = await brotliDecompress(value)
  return Buffer.from(uncompressedContent);
}


