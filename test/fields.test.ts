import { assert } from 'chai';
import { ParquetSchema } from '../parquet';
import * as fields from '../lib/fields';

describe('Field Builders: Primitive Types', function () {
  it('Can use primitive field types: String', function () {
    const schema = new ParquetSchema({
      name: fields.createStringField(),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'BYTE_ARRAY');
    assert.equal(c.originalType, 'UTF8');
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Boolean', function () {
    const schema = new ParquetSchema({
      name: fields.createBooleanField(),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'BOOLEAN');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Int32', function () {
    const schema = new ParquetSchema({
      name: fields.createIntField(32),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'INT32');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Int64', function () {
    const schema = new ParquetSchema({
      name: fields.createIntField(64),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'INT64');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Float', function () {
    const schema = new ParquetSchema({
      name: fields.createFloatField(),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'FLOAT');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Double', function () {
    const schema = new ParquetSchema({
      name: fields.createDoubleField(),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'DOUBLE');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Decimal', function () {
    const schema = new ParquetSchema({
      name: fields.createDecimalField(3),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'FLOAT');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(c.precision, 3);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });

  it('Can use primitive field types: Timestamp', function () {
    const schema = new ParquetSchema({
      name: fields.createTimestampField(),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'INT64');
    assert.equal(c.originalType, 'TIMESTAMP_MILLIS');
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  });
});

describe('Field Builders: Primitive Type options', function () {
  it('Can be required', function () {
    const schema = new ParquetSchema({
      name: fields.createStringField(false),
    });
    const c = schema.fields.name;
    assert.equal(c.repetitionType, 'REQUIRED');
  });

  it('Can be compressed', function () {
    const schema = new ParquetSchema({
      name: fields.createStringField(true, { compression: 'GZIP' }),
    });
    const c = schema.fields.name;
    assert.equal(c.compression, 'GZIP');
  });
});

describe('Field Builders: Structs and Struct List', function () {
  it('Struct Field', function () {
    const schema = new ParquetSchema({
      name: fields.createStructField({
        foo: fields.createStringField(),
        bar: fields.createStringField(),
      }),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, undefined);
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, undefined);
    assert.equal(c.compression, undefined);
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, true);
    assert.equal(c.fieldCount, 2);
  });

  it('Struct List Field', function () {
    const schema = new ParquetSchema({
      name: fields.createStructListField({
        foo: fields.createStringField(),
        bar: fields.createStringField(),
      }),
    });
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, undefined);
    assert.equal(c.originalType, 'LIST');
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, undefined);
    assert.equal(c.compression, undefined);
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, true);
    assert.equal(c.fieldCount, 1);
  });
});

describe('Field Builders: Lists', function () {
  it('List Field', function () {
    const schema = new ParquetSchema({
      name: fields.createListField('UTF8'),
    });
    const c = schema.fields.name;

    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, undefined);
    assert.equal(c.originalType, 'LIST');
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, undefined);
    assert.equal(c.compression, undefined);
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, true);
    assert.equal(c.fieldCount, 1);
  });

  it('list field and elements can be required', function () {
    const schema = new ParquetSchema({
      group_name: fields.createListField('UTF8', false, { optional: false }),
    });
    const groupNameMeta = schema.fields.group_name;
    assert.equal(groupNameMeta.repetitionType, 'REQUIRED');
    assert.equal(groupNameMeta.name, 'group_name');

    const groupNameListMeta = schema.fieldList[1];
    assert.equal(groupNameListMeta.repetitionType, 'REPEATED');
    assert.equal(groupNameListMeta.name, 'list');

    const groupNameElementsMeta = schema.fieldList[2];
    assert.equal(groupNameElementsMeta.name, 'element');
    assert.equal(groupNameElementsMeta.repetitionType, 'REQUIRED');
    assert.equal(groupNameElementsMeta.primitiveType, 'BYTE_ARRAY');
  });
});
