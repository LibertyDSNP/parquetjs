import fs from 'fs';
import path from 'path';
import { expect } from "chai";
import { JSONSchema4 } from 'json-schema';
import addressSchema from './test-files/address.schema.json';
import arraySchema from './test-files/array.schema.json';
import objectSchema from './test-files/object.schema.json';
import objectNestedSchema from './test-files/object-nested.schema.json';

import { ParquetSchema } from '../parquet';

const update = false;
// Super Simple snapshot testing
const checkSnapshot = (actual: any, snapshot: string, update = false) => {
  if (update) {
    fs.writeFileSync(path.resolve("test", snapshot), JSON.stringify(JSON.parse(JSON.stringify(actual)), null, 2)+ "\n");
    expect(`Updated the contents of "${snapshot}"`).to.equal("");
  } else {
    const expected = require(snapshot);
    expect(JSON.parse(JSON.stringify(actual))).to.deep.equal(expected);
  }
}

describe("Json Schema Conversion", function () {
  const update = false;
  it("Simple Schema", function () {
    const js = addressSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/address.schema.result.json', update);
  });

  it("Arrays", function () {
    const js = arraySchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/array.schema.result.json', update);
  });

  it("Objects", function () {
    const js = objectSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/object.schema.result.json', update);
  });

  it("Nested Objects", function () {
    const js = objectNestedSchema as JSONSchema4;

    const ps = ParquetSchema.fromJsonSchema(js);
    checkSnapshot(ps, './test-files/object-nested.schema.result.json', update);
  });
});
