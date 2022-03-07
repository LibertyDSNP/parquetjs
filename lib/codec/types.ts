import { PrimitiveType } from "lib/types/types";
import { ParquetCodec } from "lib/types/types";

export interface Options {
    typeLength: number,
    bitWidth: number,
    disableEnvelope: boolean
    primitiveType?: PrimitiveType;
    encoding?: ParquetCodec;
}
  
export interface Cursor {
    buffer: Buffer,
    offset: number,
    size?: number,
}