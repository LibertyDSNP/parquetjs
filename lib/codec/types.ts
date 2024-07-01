import { PrimitiveType } from '../declare';
import { ParquetCodec, OriginalType, ParquetField } from '../declare';
import { Statistics } from '../../gen-nodejs/parquet_types';

export interface Options {
  typeLength: number;
  bitWidth: number;
  disableEnvelope?: boolean;
  primitiveType?: PrimitiveType;
  originalType?: OriginalType;
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
}

export interface Cursor {
  buffer: Buffer;
  offset: number;
  size?: number;
}
