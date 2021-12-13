import * as parquet_codec from './codec';
import * as parquet_compression from './compression'
import * as parquet_types from './types'
import { SchemaDefinition, ParquetField, RepetitionType } from './types/types'

const PARQUET_COLUMN_KEY_SEPARATOR = '.';

/**
 * A parquet file schema
 */
export class ParquetSchema {
  schema: SchemaDefinition
  fields: Record<string, ParquetField>
  fieldList: Array<ParquetField>

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
  findField(path: string | Array<string>) {
    if (typeof path === 'string') {
      path = path.split(",");
    } else {
      path = path.slice(0); // clone array
    }

    let n = this.fields;
    for (; path.length > 1; path.shift()) {
      let field = n[path[0]].fields
      if (isDefined(field)) {
        n = field;
      }
    }

    return n[path[0]];
  }

  /**
   * Retrieve a field definition and all the field's ancestors
   */
  findFieldBranch(path: string | Array<string>) {
    if (typeof path === 'string') {
      path = path.split(",");
    }

    let branch = [];
    let n = this.fields;
    for (; path.length > 0; path.shift()) {
      branch.push(n[path[0]]);

      let field = n[path[0]].fields
      if (path.length > 1 && isDefined(field)) {
        n = field;
      }
    }

    return branch;
  }

};

function buildFields(schema: SchemaDefinition, rLevelParentMax?: number, dLevelParentMax?: number, path?: Array<string>) {
  if (!rLevelParentMax) {
    rLevelParentMax = 0;
  }

  if (!dLevelParentMax) {
    dLevelParentMax = 0;
  }

  if (!path) {
    path = [];
  }

  let fieldList: Record<string, ParquetField> = {};
  for (let name in schema) {
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
        fields: buildFields(
              opts.fields,
              rLevelMax,
              dLevelMax,
              path.concat(name))
      };
      
      continue;
    }

    let typeDef ;
    /* field type */
    if (!opts.type) {
      throw 'invalid parquet type: ' + opts.type;
    } else {
      typeDef = parquet_types.PARQUET_LOGICAL_TYPES[opts.type];
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = 'PLAIN';
    }

    if (!(opts.encoding in parquet_codec)) {
      throw 'unsupported parquet encoding: ' + opts.encoding;
    }

    if (!opts.compression) {
      opts.compression = 'UNCOMPRESSED';
    }

    if (!(opts.compression in parquet_compression.PARQUET_COMPRESSION_METHODS)) {
      throw 'unsupported compression method: ' + opts.compression;
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
      typeLength: opts.typeLength || typeDef.typeLength,
      rLevelMax: rLevelMax,
      dLevelMax: dLevelMax
    };
  }

  return fieldList;
}

function listFields(fields: Record<string, ParquetField>) {
  let list: Array<ParquetField> = [];

  for (let k in fields) {
    list.push(fields[k]);

    let field = fields[k].fields
    if (fields[k].isNested && isDefined(field)) {
      list = list.concat(listFields(field));
    }
  }

  return list;
}

function isDefined<T>(val: T | undefined): val is T {
  return val !== undefined;
}
