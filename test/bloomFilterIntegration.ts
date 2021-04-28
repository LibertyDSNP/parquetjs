import { assert }from "chai";
const parquet = require("../parquet.js");
const TEST_VTIME = new Date();

type BloomFilters = {
  name: Array<any>
  quantity: Array<any>
};

const schema = new parquet.ParquetSchema({
  name: { type: "UTF8" },
  quantity: { type: "INT64", optional: true },
  price: { type: "DOUBLE" },
  date: { type: "TIMESTAMP_MICROS" },
  day: { type: "DATE" },
  finger: { type: "FIXED_LEN_BYTE_ARRAY", typeLength: 5 },
  inter: { type: "INTERVAL", statistics: false },
  stock: {
    repeated: true,
    fields: {
      quantity: { type: "INT64", repeated: true },
      warehouse: { type: "UTF8" },
    },
  },
  colour: { type: "UTF8", repeated: true },
  meta_json: { type: "BSON", optional: true, statistics: false },
});

describe("bloom filter", async function () {
  let row: any;
  let reader: any;
  let bloomFilters: BloomFilters;

  before(async function () {
    const options = {
      pageSize: 3,
      bloomFilters: [
        {
          column: "name",
          numFilterBytes: 1024,
        },
        {
          column: "quantity",
          numFilterBytes: 1024,
        },
      ],
    };

    let writer = await parquet.ParquetWriter.openFile(
      schema,
      "fruits-bloomfilter.parquet",
      options
    );

    await writer.appendRow({
      name: "apples",
      quantity: BigInt(10),
      price: 2.6,
      day: new Date("2017-11-26"),
      date: new Date(TEST_VTIME.valueOf() + 1000),
      finger: "FNORD",
      inter: { months: 10, days: 5, milliseconds: 777 },
      colour: ["green", "red"],
    });

    await writer.appendRow({
      name: "oranges",
      quantity: BigInt(20),
      price: 2.7,
      day: new Date("2018-03-03"),
      date: new Date(TEST_VTIME.valueOf() + 2000),
      finger: "ABCDE",
      inter: { months: 42, days: 23, milliseconds: 777 },
      colour: ["orange"],
    });

    await writer.appendRow({
      name: "kiwi",
      price: 4.2,
      quantity: BigInt(15),
      day: new Date("2008-11-26"),
      date: new Date(TEST_VTIME.valueOf() + 8000),
      finger: "XCVBN",
      inter: { months: 60, days: 1, milliseconds: 99 },
      stock: [
        { quantity: BigInt(42), warehouse: "f" },
        { quantity: BigInt(21), warehouse: "x" },
      ],
      colour: ["green", "brown", "yellow"],
      meta_json: { expected_ship_date: TEST_VTIME.valueOf() },
    });

    await writer.appendRow({
      name: "banana",
      price: 3.2,
      day: new Date("2017-11-26"),
      date: new Date(TEST_VTIME.valueOf() + 6000),
      finger: "FNORD",
      inter: { months: 1, days: 15, milliseconds: 888 },
      colour: ["yellow"],
      meta_json: { shape: "curved" },
    });

    await writer.close();
    reader = await parquet.ParquetReader.openFile("fruits-bloomfilter.parquet");
    row = reader.metadata.row_groups[0];

    bloomFilters = await reader.getBloomFiltersFor(["name", "quantity"]);
  });

  it('contains name and quantity filter', () => {
    const columnsFilterNames = Object.keys(bloomFilters);
    assert.deepEqual(columnsFilterNames, ['name', 'quantity']);
  });

  it("writes bloom filters for column: name", async function () {
    const splitBlockBloomFilter = bloomFilters.name[0].sbbf;
    assert.isTrue(
      splitBlockBloomFilter.check(Buffer.from("apples")),
      "apples is included in name filter"
    );
    assert.isTrue(
      splitBlockBloomFilter.check(Buffer.from("oranges")),
      "oranges is included in name filter"
    );
    assert.isTrue(
      splitBlockBloomFilter.check(Buffer.from("kiwi")),
      "kiwi is included"
    );
    assert.isTrue(
      splitBlockBloomFilter.check(Buffer.from("banana")),
      "banana is included in name filter"
    );
    assert.isFalse(
      splitBlockBloomFilter.check(Buffer.from("taco")),
      "taco is NOT included in name filter"
    );
  });

  it("writes bloom filters for column: quantity", async function () {
    const splitBlockBloomFilter = bloomFilters.quantity[0].sbbf;
    assert.isTrue(
      splitBlockBloomFilter.check(BigInt(10)),
      "10n is included in quantity filter"
    );
    assert.isTrue(
      splitBlockBloomFilter.check(BigInt(15)),
      "15n is included in quantity filter"
    );
    assert.isFalse(
      splitBlockBloomFilter.check(BigInt(100)),
      "100n is NOT included in quantity filter"
    );
  });
});
