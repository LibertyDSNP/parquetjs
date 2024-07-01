import fs from 'fs';
import path from 'path';
import { assert, expect } from 'chai';
import { JSONSchema4 } from 'json-schema';
import addressSchema from './test-files/address.schema.json';
import arraySchema from './test-files/array.schema.json';
import objectSchema from './test-files/object.schema.json';
import objectNestedSchema from './test-files/object-nested.schema.json';

import { ParquetSchema, ParquetWriter, ParquetReader } from '../parquet';

const update = false;
// Super Simple snapshot testing
const checkSnapshot = (actual: any, snapshot: string, update = false) => {
  if (update) {
    fs.writeFileSync(
      path.resolve('test', snapshot),
      JSON.stringify(JSON.parse(JSON.stringify(actual)), null, 2) + '\n'
    );
    expect(`Updated the contents of "${snapshot}"`).to.equal('');
  } else {
    const expected = require(snapshot);
    expect(JSON.parse(JSON.stringify(actual))).to.deep.equal(expected);
  }
};

describe('Json Schema Conversion', function () {
  it('Simple Schema', function () {
    const js = addressSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/address.schema.result.json', update);
  });

  it('Arrays', function () {
    const js = arraySchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/array.schema.result.json', update);
  });

  it('Objects', function () {
    const js = objectSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/object.schema.result.json', update);
  });

  it('Nested Objects', function () {
    const js = objectNestedSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/object-nested.schema.result.json', update);
  });
});

const parquetSchema = ParquetSchema.fromJsonSchema({
  type: 'object',
  properties: {
    string_field: { type: 'string' },
    int_field: { type: 'integer' },
    number_field: { type: 'number' },
    array_field: {
      type: 'array',
      items: { type: 'string' },
      additionalItems: false,
    },
    timestamp_array_field: {
      type: 'array',
      items: {
        type: 'string',
        format: 'date-time',
      },
      additionalItems: false,
    },
    timestamp_field: {
      type: 'string',
      format: 'date-time',
    },
    obj_field: {
      type: 'object',
      properties: {
        sub1: {
          type: 'string',
        },
        sub2: {
          type: 'string',
        },
      },
      additionalProperties: false,
    },
    struct_field: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          sub3: { type: 'string' },
          sub4: { type: 'string' },
          sub5: {
            type: 'object',
            properties: {
              sub6: { type: 'string' },
              sub7: { type: 'string' },
            },
            additionalProperties: false,
          },
          sub8: {
            type: 'array',
            items: { type: 'string' },
          },
        },
        additionalProperties: false,
      },
      additionalItems: false,
    },
  },
  additionalProperties: false,
});

describe('Json Schema Conversion Test File', function () {
  const row1 = {
    string_field: 'string value',
    int_field: 10n,
    number_field: 2.5,
    timestamp_array_field: { list: [{ element: new Date('2023-01-01 GMT') }] },

    timestamp_field: new Date('2023-01-01 GMT'),

    array_field: {
      list: [{ element: 'array_field val1' }, { element: 'array_field val2' }],
    },

    obj_field: {
      sub1: 'obj_field_sub1 val',
      sub2: 'obj_field_sub2 val',
    },

    struct_field: {
      list: [
        {
          element: {
            sub8: {
              list: [{ element: 'val1' }, { element: 'val2' }],
            },
            sub3: 'struct_field_string val',
            sub4: 'struct_field_string val',
            sub5: {
              sub6: 'struct_field_struct_string1 val',
              sub7: 'struct_field_struct_string2 val',
            },
          },
        },
      ],
    },
  };

  let reader: ParquetReader;

  before(async function () {
    const filename = 'json-schema-test-file.parquet';
    const writer = await ParquetWriter.openFile(parquetSchema, filename);
    await writer.appendRow(row1);
    await writer.close();

    reader = await ParquetReader.openFile(filename);
  });

  it('schema is generated correctly', async function () {
    checkSnapshot(parquetSchema, './test-files/json-schema-test-file.schema.result.json', update);
  });

  it('schema is encoded correctly', async function () {
    const schema = reader.metadata?.schema;
    checkSnapshot(schema, './test-files/json-schema-test-file.result.json', update);
  });

  it('output matches input', async function () {
    const cursor = reader.getCursor();
    const row = await cursor.next();
    const rowData = {
      ...row1,
    };
    assert.deepEqual(row, rowData);
  });
});
