// Lifted from https://github.com/kbajalc/parquets

import { RowGroup } from "gen-nodejs/parquet_types";
import SplitBlockBloomFilter from "lib/bloom/sbbf";

export type ParquetCodec = 'PLAIN' | 'RLE';
export type ParquetCompression = 'UNCOMPRESSED' | 'GZIP' | 'SNAPPY' | 'LZO' | 'BROTLI' | 'LZ4';
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
export type ParquetType = PrimitiveType | OriginalType;

export type PrimitiveType =
// Base Types
    |'BOOLEAN' // 0
    | 'INT32' // 1
    | 'INT64' // 2
    | 'INT96' // 3
    | 'FLOAT' // 4
    | 'DOUBLE' // 5
    | 'BYTE_ARRAY' // 6,
    | 'FIXED_LEN_BYTE_ARRAY'; // 7

export type OriginalType =
// Converted Types
    | 'UTF8' // 0
    | 'MAP' // 1
    // | 'MAP_KEY_VALUE' // 2
     | 'LIST' // 3
    // | 'ENUM' // 4
    // | 'DECIMAL' // 5
    | 'DATE' // 6
    | 'TIME_MILLIS' // 7
    | 'TIME_MICROS' // 8
    | 'TIMESTAMP_MILLIS' // 9
    | 'TIMESTAMP_MICROS' // 10
    | 'UINT_8' // 11
    | 'UINT_16' // 12
    | 'UINT_32' // 13
    | 'UINT_64' // 14
    | 'INT_8' // 15
    | 'INT_16' // 16
    | 'INT_32' // 17
    | 'INT_64' // 18
    | 'JSON' // 19
    | 'BSON' // 20
    | 'INTERVAL'; // 21

export interface Statistics {
    max: number
    min: number
    null_count: number
    distinct_count: number
    max_value: number
    min_value: number
}

export interface SchemaDefinition {
    [string: string]: FieldDefinition;
}

export interface FieldDefinition {
    type?: ParquetType;
    typeLength?: number;
    encoding?: ParquetCodec;
    compression?: ParquetCompression;
    optional?: boolean;
    repeated?: boolean;
    fields?: SchemaDefinition;
    statistics?: Statistics
}

export interface ParquetField {
    name: string;
    path: string[];
    statistics?: Statistics
    primitiveType?: PrimitiveType;
    originalType?: OriginalType;
    repetitionType: RepetitionType;
    typeLength?: number;
    encoding?: ParquetCodec;
    compression?: ParquetCompression;
    rLevelMax: number;
    dLevelMax: number;
    isNested?: boolean;
    fieldCount?: number;
    fields?: Record<string, ParquetField>;
}

export interface ParquetBuffer {
    rowCount?: number;
    columnData?: Record<string, ParquetData>;
}

export interface ParquetData {
    dlevels: number[];
    rlevels: number[];
    values: any[];
    count: number;
}

export interface ParquetRecord {
    [key: string]: any;
}

export interface Offset {
    buffer: Buffer
    offset: number
}

export interface ColumnMetaData {
    type: number,
    encodings: Array<any>,
    path_in_schema: Array<string>,
    codec: number,
    num_values: any,
    total_uncompressed_size: any,
    total_compressed_size: any,
    key_value_metadata: any,
    data_page_offset: Offset,
    index_page_offset: Offset,
    dictionary_page_offset: Offset,
    statistics: any,
    encoding_stats: any,
    bloom_filter_offset: Offset
}

export interface ColumnData {
    file_path: string,
    file_offset: Offset,
    meta_data: ColumnMetaData
}

export interface ColumnChunkData {
    rowGroupIndex: number,
    column: ColumnData
}

export type Block = Uint32Array

export interface BloomFilterData {
    sbbf: SplitBlockBloomFilter,
    columnName: string,
    RowGroupIndex: number,
};
