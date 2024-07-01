'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet');

describe('ParquetSchema', function () {
  it('should assign correct defaults in a simple flat schema', function () {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.fieldList.length, 3);
    assert(schema.fields.name);
    assert(schema.fields.quantity);
    assert(schema.fields.price);

    {
      const c = schema.fields.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['quantity']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }
  });

  it('should assign correct defaults in a flat schema with optional fieldList', function () {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', optional: true },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.fieldList.length, 3);
    assert(schema.fields.name);
    assert(schema.fields.quantity);
    assert(schema.fields.price);

    {
      const c = schema.fields.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['quantity']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 1);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }
  });

  it('should assign correct defaults in a flat schema with repeated fieldList', function () {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', repeated: true },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.fieldList.length, 3);
    assert(schema.fields.name);
    assert(schema.fields.quantity);
    assert(schema.fields.price);

    {
      const c = schema.fields.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['quantity']);
      assert.equal(c.repetitionType, 'REPEATED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 1);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }
  });

  it('should assign correct defaults in a nested schema without repetition modifiers', function () {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        fields: {
          quantity: { type: 'INT64' },
          warehouse: { type: 'UTF8' },
        },
      },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.fieldList.length, 5);
    assert(schema.fields.name);
    assert(schema.fields.stock);
    assert(schema.fields.stock.fields.quantity);
    assert(schema.fields.stock.fields.warehouse);
    assert(schema.fields.price);

    {
      const c = schema.fields.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.stock;
      assert.equal(c.name, 'stock');
      assert.equal(c.primitiveType, undefined);
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, undefined);
      assert.equal(c.compression, undefined);
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, true);
      assert.equal(c.fieldCount, 2);
    }

    {
      const c = schema.fields.stock.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock', 'quantity']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.stock.fields.warehouse;
      assert.equal(c.name, 'warehouse');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['stock', 'warehouse']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }
  });

  it('should assign correct defaults in a nested schema with optional fields', function () {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        optional: true,
        fields: {
          quantity: { type: 'INT64', optional: true },
          warehouse: { type: 'UTF8' },
        },
      },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.fieldList.length, 5);
    assert(schema.fields.name);
    assert(schema.fields.stock);
    assert(schema.fields.stock.fields.quantity);
    assert(schema.fields.stock.fields.warehouse);
    assert(schema.fields.price);

    {
      const c = schema.fields.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.stock;
      assert.equal(c.name, 'stock');
      assert.equal(c.primitiveType, undefined);
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, undefined);
      assert.equal(c.compression, undefined);
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 1);
      assert.equal(!!c.isNested, true);
      assert.equal(c.fieldCount, 2);
    }

    {
      const c = schema.fields.stock.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock', 'quantity']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 2);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.stock.fields.warehouse;
      assert.equal(c.name, 'warehouse');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['stock', 'warehouse']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 1);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }
  });

  it('should assign correct defaults in a nested schema with repeated fields', function () {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        repeated: true,
        fields: {
          quantity: { type: 'INT64', optional: true },
          warehouse: { type: 'UTF8' },
        },
      },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.fieldList.length, 5);
    assert(schema.fields.name);
    assert(schema.fields.stock);
    assert(schema.fields.stock.fields.quantity);
    assert(schema.fields.stock.fields.warehouse);
    assert(schema.fields.price);

    {
      const c = schema.fields.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.stock;
      assert.equal(c.name, 'stock');
      assert.equal(c.primitiveType, undefined);
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock']);
      assert.equal(c.repetitionType, 'REPEATED');
      assert.equal(c.encoding, undefined);
      assert.equal(c.compression, undefined);
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 1);
      assert.equal(!!c.isNested, true);
      assert.equal(c.fieldCount, 2);
    }

    {
      const c = schema.fields.stock.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock', 'quantity']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 2);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.stock.fields.warehouse;
      assert.equal(c.name, 'warehouse');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['stock', 'warehouse']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 1);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }

    {
      const c = schema.fields.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
      assert.equal(!!c.isNested, false);
      assert.equal(c.fieldCount, undefined);
    }
  });

  it('should indicate which column had an invalid type in a simple flat schema', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        quantity: { type: 'UNKNOWN' },
      });
    }, 'Invalid parquet type: UNKNOWN, for Column: quantity');
  });

  it('should indicate each column which has an invalid type in a simple flat schema', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        quantity: { type: 'UNKNOWN' },
        value: { type: 'UNKNOWN' },
      });
    }, 'Invalid parquet type: UNKNOWN, for Column: quantity\nInvalid parquet type: UNKNOWN, for Column: value');
  });

  it('should indicate each column which has an invalid type when one is correct in a simple flat schema', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        quantity: { type: 'INT32' },
        value: { type: 'UNKNOWN' },
      });
    }, 'Invalid parquet type: UNKNOWN, for Column: value');
  });

  it('should indicate each column which has an invalid type in a nested schema', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        name: { type: 'UTF8' },
        stock: {
          fields: {
            quantity: { type: 'UNKNOWN' },
            warehouse: { type: 'UNKNOWN' },
          },
        },
        price: { type: 'UNKNOWN' },
      });
    }, 'Invalid parquet type: UNKNOWN, for Column: stock.quantity\nInvalid parquet type: UNKNOWN, for Column: stock.warehouse');
  });

  it('should indicate which column had an invalid encoding in a simple flat schema', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        quantity: { type: 'INT32', encoding: 'UNKNOWN' },
      });
    }, 'Unsupported parquet encoding: UNKNOWN, for Column: quantity');
  });

  it('should indicate which column had an invalid compression type in a simple flat schema', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        quantity: { type: 'INT32', compression: 'UNKNOWN' },
      });
    }, 'Unsupported compression method: UNKNOWN, for Column: quantity');
  });

  it('should throw error given decimal with no precision', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        test_decimal_col: { type: 'DECIMAL', scale: 4 },
      });
    }, 'invalid schema for type: DECIMAL, for Column: test_decimal_col, precision is required and must be be greater than 0');
  });

  it('should NOT throw error given decimal with no scale', function () {
    assert.doesNotThrow(() => {
      new parquet.ParquetSchema({
        test_decimal_col: { type: 'DECIMAL', precision: 4 },
      });
    });
  });

  it('should throw error given decimal with negative precision', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        decimal_column: { type: 'DECIMAL', precision: -1, scale: 0 },
      });
    }, 'invalid schema for type: DECIMAL, for Column: decimal_column, precision is required and must be be greater than 0');
  });

  it('should throw error given decimal with a non-integer precision', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        decimal_column: { type: 'DECIMAL', precision: 6.1, scale: 5 },
      });
    }, 'invalid schema for type: DECIMAL, for Column: decimal_column, precision must be an integer');
  });

  it('should throw error given decimal with a non-integer scale', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        decimal_column: { type: 'DECIMAL', precision: 6, scale: 5.1 },
      });
    }, 'invalid schema for type: DECIMAL, for Column: decimal_column, scale must be an integer');
  });

  it('should throw error given decimal with negative scale', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        decimal_column: { type: 'DECIMAL', precision: 6, scale: -1 },
      });
    }, 'invalid schema for type: DECIMAL, for Column: decimal_column, scale is required to be 0 or greater');
  });

  it('should throw error given decimal with scale > precision', function () {
    assert.throws(() => {
      new parquet.ParquetSchema({
        decimal_column: { type: 'DECIMAL', precision: 5, scale: 6 },
      });
    }, 'invalid schema or precision for type: DECIMAL, for Column: decimal_column, precision must be greater than or equal to scale');
  });
});
