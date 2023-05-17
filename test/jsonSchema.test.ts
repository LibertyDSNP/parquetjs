import { expect } from "chai";
import { JSONSchema4 } from 'json-schema';
import addressSchema from './test-files/address.schema.json';
import arraySchema from './test-files/array.schema.json';
import objectSchema from './test-files/object.schema.json';
import objectNestedSchema from './test-files/object-nested.schema.json';

import { ParquetSchema } from '../parquet';

// Super Simple snapshot testing
const checkSnapshot = (actual: any, snapshot: string, update = false) => {
  const expected = require(snapshot);
  if (update) {
    console.log(`Replace the contents of ${snapshot} with:\n`, JSON.stringify(JSON.parse(JSON.stringify(actual)), null, 2));
    expect("See output").to.equal("");
  } else {
    expect(JSON.parse(JSON.stringify(actual))).to.deep.equal(expected);
  }
}

describe("Json Schema Conversion", function () {
  it("Simple Schema", function () {
    const js = addressSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/address.schema.result.json');
  });

  it("Arrays", function () {
    const js = arraySchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/array.schema.result.json');
  });

  it("Objects", function () {
    const js = objectSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/object.schema.result.json');
  });

  it("Nested Objects", function () {
    const js = objectNestedSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/object-nested.schema.result.json');
  });
});
