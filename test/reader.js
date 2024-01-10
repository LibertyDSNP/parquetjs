"use strict";
const chai = require("chai");
const path = require("path");
const assert = chai.assert;
const parquet = require("../parquet");
const server = require("./mocks/server");

describe("ParquetReader", () => {
  describe("#openUrl", () => {
    before(() => {
      server.listen();
    });

    afterEach(() => {
      server.resetHandlers();
    });

    after(() => {
      server.close();
    });

    it("reads parquet files via http", async () => {
      const reader = await parquet.ParquetReader.openUrl(
        "http://fruits-bloomfilter.parquet"
      );

      const cursor = await reader.getCursor();

      assert.deepOwnInclude(
        await cursor.next(),
        {
          name: "apples",
          quantity: 10n,
          price: 2.6,
          day: new Date("2017-11-26"),
          finger: Buffer.from("FNORD"),
          inter: {months: 10, days: 5, milliseconds: 777},
          colour: ["green", "red"],
        }
      );

      assert.deepOwnInclude(
        await cursor.next(),
        {
          name: "oranges",
          quantity: 20n,
          price: 2.7,
          day: new Date("2018-03-03"),
          finger: Buffer.from("ABCDE"),
          inter: {months: 42, days: 23, milliseconds: 777},
          colour: ["orange"],
        }
      );

      assert.deepOwnInclude(
        await cursor.next(),
        {
          name: "kiwi",
          quantity: 15n,
          price: 4.2,
          day: new Date("2008-11-26"),
          finger: Buffer.from("XCVBN"),
          inter: {months: 60, days: 1, milliseconds: 99},
          colour: ["green", "brown", "yellow"],
          stock: [
            {
              quantity: [42n],
              warehouse: "f",
            },
            {
              quantity: [21n],
              warehouse: "x",
            },
          ]
        }
      );

      assert.deepOwnInclude(
        await cursor.next(),
        {
          name: "banana",
          price: 3.2,
          day: new Date("2017-11-26"),
          finger: Buffer.from("FNORD"),
          inter: {months: 1, days: 15, milliseconds: 888},
          colour: ["yellow"],
          meta_json: {
            shape: "curved",
          },
        }
      );

      assert.deepEqual(null, await cursor.next());
    });
  });

  describe("#asyncIterator", () => {
    it("responds to for await", async () => {
      const reader = await parquet.ParquetReader.openFile(
        path.join(__dirname, 'test-files', 'fruits.parquet')
      );

      let counter = 0;
      for await(const record of reader) {
        counter++;
      }

      assert.equal(counter, 40000);
    })
  });

  describe("#handleDecimal", () => {
    it("loads parquet with columns configured as DECIMAL", async () => {
      const reader = await parquet.ParquetReader.openFile(
        path.join(__dirname, 'test-files', 'valid-decimal-columns.parquet')
      );

      const data = []
      for await(const record of reader) {
        data.push(record)
      }

      assert.equal(data.length, 4);
      assert.equal(data[0].over_9_digits, 118.0297106);
      assert.equal(data[1].under_9_digits, 18.7106);
      // handling null values
      assert.equal(data[2].over_9_digits, undefined);
      assert.equal(data[2].under_9_digits, undefined);
    })
  });

  describe('#openS3', () => {

    describe("S3V2",() => {});
    describe("S3V3",() => {
      it('works', async () => {

      })
    });
  });
});
