import { PrimitiveType } from '../declare';
import { ParquetCodec, OriginalType, ParquetField } from '../declare';
import { LogicalType, Statistics } from '../../gen-nodejs/parquet_types';

export interface Options {
  typeLength: number;
  bitWidth: number;
  disableEnvelope?: boolean;
  primitiveType?: PrimitiveType;
  originalType?: OriginalType;
  logicalType?: LogicalType;
  encoding?: ParquetCodec;
  compression?: string;
  column?: ParquetField;
  rawStatistics?: Statistics;
  cache?: unknown;
  dictionary?: number[];
  num_values?: number;
  rLevelMax?: number;
  dLevelMax?: number;
  type?: string;
  name?: string;
  precision?: number;
  scale?: number;
  treatInt96AsTimestamp?: boolean;
}

export interface Cursor {
  buffer: Buffer;
  offset: number;
  size?: number;
}

export interface DataReader {
    view: DataView
    offset: number
}

export type DecodedArray =
  Uint8Array |
  Int32Array |
  BigInt64Array |
  BigUint64Array |
  Float32Array |
  Float64Array |
  any[]

