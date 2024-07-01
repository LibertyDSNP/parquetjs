'use strict';
const chai = require('chai');
const path = require('path');
const assert = chai.assert;
const parquet = require('../parquet');
const server = require('./mocks/server');
const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');
const { sdkStreamMixin } = require('@smithy/util-stream');
const { createReadStream } = require('fs');
const { ParquetReader } = require('../parquet');

const s3Mock = mockClient(S3Client);

describe('ParquetReader', function () {
  describe('#openUrl', function () {
    before(function () {
      server.listen();
    });

    afterEach(function () {
      server.resetHandlers();
    });

    after(function () {
      server.close();
    });

    it('reads parquet files via http', async function () {
      const reader = await parquet.ParquetReader.openUrl('http://fruits-bloomfilter.parquet');

      const cursor = await reader.getCursor();

      assert.deepOwnInclude(await cursor.next(), {
        name: 'apples',
        quantity: 10n,
        price: 2.6,
        day: new Date('2017-11-26'),
        finger: Buffer.from('FNORD'),
        inter: { months: 10, days: 5, milliseconds: 777 },
        colour: ['green', 'red'],
      });

      assert.deepOwnInclude(await cursor.next(), {
        name: 'oranges',
        quantity: 20n,
        price: 2.7,
        day: new Date('2018-03-03'),
        finger: Buffer.from('ABCDE'),
        inter: { months: 42, days: 23, milliseconds: 777 },
        colour: ['orange'],
      });

      assert.deepOwnInclude(await cursor.next(), {
        name: 'kiwi',
        quantity: 15n,
        price: 4.2,
        day: new Date('2008-11-26'),
        finger: Buffer.from('XCVBN'),
        inter: { months: 60, days: 1, milliseconds: 99 },
        colour: ['green', 'brown', 'yellow'],
        stock: [
          {
            quantity: [42n],
            warehouse: 'f',
          },
          {
            quantity: [21n],
            warehouse: 'x',
          },
        ],
      });

      assert.deepOwnInclude(await cursor.next(), {
        name: 'banana',
        price: 3.2,
        day: new Date('2017-11-26'),
        finger: Buffer.from('FNORD'),
        inter: { months: 1, days: 15, milliseconds: 888 },
        colour: ['yellow'],
        meta_json: {
          shape: 'curved',
        },
      });

      assert.deepEqual(null, await cursor.next());
    });
  });

  describe('#asyncIterator', function () {
    it('responds to for await', async function () {
      const reader = await parquet.ParquetReader.openFile(path.join(__dirname, 'test-files', 'fruits.parquet'));

      let counter = 0;
      for await (const _record of reader) {
        counter++;
      }

      assert.equal(counter, 40000);
    });
  });

  describe('#handleDecimal', function () {
    it('loads parquet with columns configured as DECIMAL', async function () {
      const reader = await parquet.ParquetReader.openFile(
        path.join(__dirname, 'test-files', 'valid-decimal-columns.parquet')
      );

      const data = [];
      for await (const record of reader) {
        data.push(record);
      }

      assert.equal(data.length, 4);
      assert.equal(data[0].over_9_digits, 118.0297106);
      assert.equal(data[1].under_9_digits, 18.7106);
      // handling null values
      assert.equal(data[2].over_9_digits, undefined);
      assert.equal(data[2].under_9_digits, undefined);
    });
  });

  describe('ParquetReader with S3', function () {
    describe('V3', function () {
      it('works', async function () {
        let srcFile = 'test/test-files/nation.dict.parquet';

        const headStream = new Readable();
        headStream.push('PAR1');
        headStream.push(null);
        const headSdkStream = sdkStreamMixin(headStream);

        const footStream = createReadStream(srcFile, { start: 2842, end: 2849 });
        const footSdkStream = sdkStreamMixin(footStream);

        const metadataStream = createReadStream(srcFile, { start: 2608, end: 2841 });
        const metaDataSdkStream = sdkStreamMixin(metadataStream);

        const stream = createReadStream(srcFile);

        // wrap the Stream with SDK mixin
        const sdkStream = sdkStreamMixin(stream);

        // mock all the way down to where metadata is being read
        s3Mock.on(HeadObjectCommand).resolves({ ContentLength: 2849 });

        s3Mock.on(GetObjectCommand).resolves({ Body: sdkStream });

        s3Mock
          .on(GetObjectCommand, { Range: 'bytes=0-3', Key: 'foo', Bucket: 'bar' })
          .resolves({ Body: headSdkStream });

        s3Mock
          .on(GetObjectCommand, { Range: 'bytes=2841-2848', Key: 'foo', Bucket: 'bar' })
          .resolves({ Body: footSdkStream });

        s3Mock
          .on(GetObjectCommand, { Range: 'bytes=2607-2840', Key: 'foo', Bucket: 'bar' })
          .resolves({ Body: metaDataSdkStream });

        const s3 = new S3Client({});
        let res = await ParquetReader.openS3(s3, { Key: 'foo', Bucket: 'bar' });
        assert(res.envelopeReader);
      });
    });
  });
});
