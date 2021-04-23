'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const TEST_VTIME =  new Date();

const SplitBlockBloomFilter = require('../dist/lib/bloom/sbbf').default;

const schema = new parquet.ParquetSchema({
  name:       { type: 'UTF8' },
  quantity:   { type: 'INT64', optional: true },
  price:      { type: 'DOUBLE' },
  date:       { type: 'TIMESTAMP_MICROS' },
  day:        { type: 'DATE' },
  finger:     { type: 'FIXED_LEN_BYTE_ARRAY', typeLength: 5 },
  inter:      { type: 'INTERVAL', statistics: false },
  stock: {
    repeated: true,
    fields: {
      quantity: { type: 'INT64', repeated: true },
      warehouse: { type: 'UTF8' },
    }
  },
  colour:     { type: 'UTF8', repeated: true },
  meta_json:  { type: 'BSON', optional: true, statistics: false},
});


describe('bloom filter', async function() {
  let row, reader, splitBlockBloomFilter;

  before(async function(){
    const options = {
			pageSize: 3,
			bloomFilters: [
					{ 
						column: "name",
						numFilterBytes: 1024,
					}
				]
    };

    let writer = await parquet.ParquetWriter.openFile(schema, 'fruits-bloomfilter.parquet', options);
    
    writer.appendRow({
      name: 'apples',
      quantity: 10n,
      price: 2.6,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 1000),
      finger: "FNORD",
      inter: { months: 10, days: 5, milliseconds: 777 },
      colour: [ 'green', 'red' ]
    });

    writer.appendRow({
      name: 'oranges',
      quantity: 20n,
      price: 2.7,
      day: new Date('2018-03-03'),
      date: new Date(TEST_VTIME + 2000),
      finger: "ABCDE",
      inter: { months: 42, days: 23, milliseconds: 777 },
      colour: [ 'orange' ]
    });

    writer.appendRow({
      name: 'kiwi',
      price: 4.2,
      quantity: 15n,
      day: new Date('2008-11-26'),
      date: new Date(TEST_VTIME + 8000),
      finger: "XCVBN",
      inter: { months: 60, days: 1, milliseconds: 99 },
      stock: [
        { quantity: 42n, warehouse: "f" },
        { quantity: 21n, warehouse: "x" }
      ],
      colour: [ 'green', 'brown', 'yellow' ],
      meta_json: { expected_ship_date: TEST_VTIME }
    });

    writer.appendRow({
      name: 'banana',
      price: 3.2,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 6000),
      finger: "FNORD",
      inter: { months: 1, days: 15, milliseconds: 888 },
      colour: [ 'yellow'],
      meta_json: { shape: 'curved' }
    });

    await writer.close();
    reader = await parquet.ParquetReader.openFile('fruits-bloomfilter.parquet');
    row = reader.metadata.row_groups[0];

		const blocks = await reader.getBloomFiltersFor(['name'])
    // [ { columnName: 'name', bloomFilters: [ [Object] ] } ]
    // { name: [{bloomFiters: 0, group} ]}
    console.log("blocks[0]", blocks[0]);
		const columnBlocks = blocks[0].bloomFilters[0].filterBlocks;
		splitBlockBloomFilter = SplitBlockBloomFilter.from(columnBlocks);
  });

  it('writes bloom filters for specified column name', async function() {
		assert.isTrue(splitBlockBloomFilter.check(Buffer.from('apples')), 'apples is included');
		assert.isTrue(splitBlockBloomFilter.check(Buffer.from('oranges')), 'oranges is included bloomfilter');
		assert.isTrue(splitBlockBloomFilter.check(Buffer.from('kiwi')), 'kiwi is included');
		assert.isTrue(splitBlockBloomFilter.check(Buffer.from('banana')), 'banana is included');
  });
});
