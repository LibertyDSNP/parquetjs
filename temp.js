const parquet = require("./parquet.js")
const assert = require('assert');

const sbbf = require('./dist/sbbf');
const Long = require('long');
var XXHash = require('xxhash');
const { hash64 } = XXHash;
const { filterInsert, initSplitBlocks, filterCheck } = sbbf;
const fs = require('fs');
const { formatDiagnostic, createNoSubstitutionTemplateLiteral } = require("typescript");

const SEED = 0x47b6137b;
const BUF_SEED = Buffer.alloc(4)
BUF_SEED.writeUInt32LE(SEED, 0);

var schema = new parquet.ParquetSchema({
    name: { type: 'UTF8' },
    quantity: { type: 'INT64' },
    price: { type: 'DOUBLE' },
    date: { type: 'TIMESTAMP_MILLIS' },
    in_stock: { type: 'BOOLEAN' }
});

  
(async () => {
    const options = {
        bloomFilters: [
          { 
            column: "name",
            numFilterBytes: 1024,
          }
        ]
    }

    /// ParquetWriter.;
    const writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet', options);
    await writer.appendRow({name: 'apples and banannas', quantity: 10, price: 2.5, date: new Date(), in_stock: true});
    await writer.appendRow({name: 'oranges', quantity: 10, price: 2.5, date: new Date(), in_stock: true});
    // await writer.appendRow({name: 'taco', quantity: 1, price: 2.5, date: new Date(), in_stock: true});
    const close = await writer.close();

    console.log('**** | writing to file done | **************************************************************************');

    const reader = await parquet.ParquetReader.openFile('./fruits.parquet');

    const blocks = await reader.getBloomFiltersFrom(['name'])
    console.log("blocks", blocks)
    // let cursor = reader.getCursor(['name']);

    await reader.close();


    // const meta = await reader.getMetadata();
    // console.log("meta", reader.metadata)
})();
