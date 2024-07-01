import * as reader from './lib/reader';
import * as writer from './lib/writer';
import * as schema from './lib/schema';
import * as shredder from './lib/shred';
import * as fields from './lib/fields';

export type ParquetEnvelopeReader = reader.ParquetEnvelopeReader;
export type ParquetReader = reader.ParquetReader;
export type ParquetEnvelopeWriter = writer.ParquetEnvelopeWriter;
export type ParquetWriter = writer.ParquetWriter;
export type ParquetTransformer = writer.ParquetTransformer;
export type ParquetSchema = schema.ParquetSchema;

export const ParquetEnvelopeReader = reader.ParquetEnvelopeReader;
export const ParquetReader = reader.ParquetReader;
export const ParquetEnvelopeWriter = writer.ParquetEnvelopeWriter;
export const ParquetWriter = writer.ParquetWriter;
export const ParquetTransformer = writer.ParquetTransformer;
export const ParquetSchema = schema.ParquetSchema;
export const ParquetShredder = shredder;
export const ParquetFieldBuilder = fields;

export default {
  ParquetEnvelopeReader,
  ParquetReader,
  ParquetEnvelopeWriter,
  ParquetWriter,
  ParquetTransformer,
  ParquetSchema,
  ParquetShredder,
  ParquetFieldBuilder,
};
