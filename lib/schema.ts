import * as parquet_codec from './codec';
import * as parquet_compression from './compression';
import * as parquet_types from './types';
import { SchemaDefinition, ParquetField, RepetitionType, FieldDefinition } from './declare';
import { JSONSchema4 } from 'json-schema';
import { fromJsonSchema } from './jsonSchema';

/**
 * A parquet file schema
 */
export class ParquetSchema {
  schema: SchemaDefinition;
  fields: Record<string, ParquetField>;
  fieldList: ParquetField[];

  /**
   * Create a new schema from JSON Schema (json-schema.org)
   */
  static fromJsonSchema(jsonSchema: JSONSchema4) {
    const schema: SchemaDefinition = fromJsonSchema(jsonSchema);
    return new ParquetSchema(schema);
  }

  /**
   * Create a new schema from a JSON schema definition
   */
  constructor(schema: SchemaDefinition) {
    this.schema = schema;
    this.fields = buildFields(schema);
    this.fieldList = listFields(this.fields);
  }

  /**
   * Retrieve a field definition
   */
  findField(path: string | string[]) {
    if (typeof path === 'string') {
      path = path.split(',');
    } else {
      path = path.slice(0); // clone array
    }

    let n = this.fields;
    for (; path.length > 1; path.shift()) {
      const fields = n[path[0]]?.fields;
      if (isDefined(fields)) {
        n = fields;
      }
    }

    return n[path[0]];
  }

  /**
   * Retrieve a field definition and all the field's ancestors
   */
  findFieldBranch(path: string | string[]) {
    if (typeof path === 'string') {
      path = path.split(',');
    }

    const branch = [];
    let n = this.fields;
    for (; path.length > 0; path.shift()) {
      branch.push(n[path[0]]);

      const fields = n[path[0]].fields;
      if (path.length > 1 && isDefined(fields)) {
        n = fields;
      }
    }

    return branch;
  }
}

function buildFields(schema: SchemaDefinition, rLevelParentMax?: number, dLevelParentMax?: number, path?: string[]) {
  if (!rLevelParentMax) {
    rLevelParentMax = 0;
  }

  if (!dLevelParentMax) {
    dLevelParentMax = 0;
  }

  if (!path) {
    path = [];
  }

  const fieldList: Record<string, ParquetField> = {};
  let fieldErrors: string[] = [];
  for (const name in schema) {
    const opts = schema[name];

    /* field repetition type */
    const required = !opts.optional;
    const repeated = !!opts.repeated;
    let rLevelMax = rLevelParentMax;
    let dLevelMax = dLevelParentMax;

    let repetitionType: RepetitionType = 'REQUIRED';
    if (!required) {
      repetitionType = 'OPTIONAL';
      ++dLevelMax;
    }

    if (repeated) {
      repetitionType = 'REPEATED';
      ++rLevelMax;

      if (required) {
        ++dLevelMax;
      }
    }

    /* nested field */

    if (opts.fields) {
      fieldList[name] = {
        name: name,
        path: path.concat(name),
        repetitionType: repetitionType,
        rLevelMax: rLevelMax,
        dLevelMax: dLevelMax,
        isNested: true,
        statistics: opts.statistics,
        fieldCount: Object.keys(opts.fields).length,
        fields: buildFields(opts.fields, rLevelMax, dLevelMax, path.concat(name)),
      };

      if (opts.type == 'LIST' || opts.type == 'MAP') fieldList[name].originalType = opts.type;

      continue;
    }

    let nameWithPath = `${name}` || 'missing name';
    if (path && path.length > 0) {
      nameWithPath = `${path}.${nameWithPath}`;
    }

    const typeDef = opts.type ? parquet_types.getParquetTypeDataObject(opts.type, opts) : undefined;
    if (!typeDef) {
      fieldErrors.push(`Invalid parquet type: ${opts.type || 'missing type'}, for Column: ${nameWithPath}`);
      continue;
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = 'PLAIN';
    }

    if (!(opts.encoding in parquet_codec)) {
      fieldErrors.push(`Unsupported parquet encoding: ${opts.encoding}, for Column: ${nameWithPath}`);
    }

    if (!opts.compression) {
      opts.compression = 'UNCOMPRESSED';
    }

    if (!(opts.compression in parquet_compression.PARQUET_COMPRESSION_METHODS)) {
      fieldErrors.push(`Unsupported compression method: ${opts.compression}, for Column: ${nameWithPath}`);
    }

    if (typeDef.originalType === 'DECIMAL') {
      // Default scale to 0 per https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
      if (typeof opts.scale === 'undefined') opts.scale = 0;
      fieldErrors = fieldErrors.concat(
        errorsForDecimalOpts(typeDef.originalType, typeDef.primitiveType, opts, nameWithPath)
      );
    }

    /* add to schema */
    fieldList[name] = {
      name: name,
      primitiveType: typeDef.primitiveType,
      originalType: typeDef.originalType,
      path: path.concat([name]),
      repetitionType: repetitionType,
      encoding: opts.encoding,
      statistics: opts.statistics,
      compression: opts.compression,
      precision: opts.precision,
      scale: opts.scale,
      typeLength: opts.typeLength || typeDef.typeLength,
      rLevelMax: rLevelMax,
      dLevelMax: dLevelMax,
    };
  }

  if (fieldErrors.length > 0) {
    throw fieldErrors.reduce((accumulator, currentVal) => accumulator + '\n' + currentVal);
  }

  return fieldList;
}

function listFields(fields: Record<string, ParquetField>) {
  let list: ParquetField[] = [];

  for (const k in fields) {
    list.push(fields[k]);

    const nestedFields = fields[k].fields;
    if (fields[k].isNested && isDefined(nestedFields)) {
      list = list.concat(listFields(nestedFields));
    }
  }

  return list;
}

function isDefined<T>(val: T | undefined): val is T {
  return val !== undefined;
}

function errorsForDecimalOpts(
  type: string,
  primitiveType: string | undefined,
  opts: FieldDefinition,
  columnName: string
): string[] {
  const fieldErrors = [];
  if (opts.precision === undefined || opts.precision < 1) {
    fieldErrors.push(
      `invalid schema for type: ${type}, for Column: ${columnName}, precision is required and must be be greater than 0`
    );
  } else if (!Number.isInteger(opts.precision)) {
    fieldErrors.push(`invalid schema for type: ${type}, for Column: ${columnName}, precision must be an integer`);
  } else if (primitiveType === 'INT64' && opts.precision > 18) {
    fieldErrors.push(
      `invalid schema for type: ${type} and primitive type: ${primitiveType} for Column: ${columnName}, can not handle precision over 18`
    );
  }
  if (typeof opts.scale === 'undefined' || opts.scale < 0) {
    fieldErrors.push(
      `invalid schema for type: ${type}, for Column: ${columnName}, scale is required to be 0 or greater`
    );
  } else if (!Number.isInteger(opts.scale)) {
    fieldErrors.push(`invalid schema for type: ${type}, for Column: ${columnName}, scale must be an integer`);
  } else if (opts.precision !== undefined && opts.scale > opts.precision) {
    fieldErrors.push(
      `invalid schema or precision for type: ${type}, for Column: ${columnName}, precision must be greater than or equal to scale`
    );
  }
  return fieldErrors;
}
