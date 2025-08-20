import { expect } from 'chai';
import { ParquetSchema } from '../lib/schema';
import { ParquetWriter } from '../lib/writer';
import { ParquetReader } from '../lib/reader';

describe('Metadata MinMax', function () {
  describe('Buffer Comparison', function () {
    it('correctly chooses the min and max', async function () {
      const decimalSchema = new ParquetSchema({
        rank: { type: 'DECIMAL' as const, precision: 20 },
      });
      const testFile = 'min-max-decimal.parquet';
      const writer = await ParquetWriter.openFile(decimalSchema, testFile);

      // These are the numbers for 10^15 + 127 through 10^15 + 129
      const min = Buffer.from([0x00, 0x00, 0x03, 0x8d, 0x7e, 0xa4, 0xc6, 0x80, 0x7f]);
      const mid = Buffer.from([0x00, 0x00, 0x03, 0x8d, 0x7e, 0xa4, 0xc6, 0x80, 0x80]);
      const max = Buffer.from([0x00, 0x00, 0x03, 0x8d, 0x7e, 0xa4, 0xc6, 0x80, 0x81]);
      await writer.appendRow({ rank: min });
      await writer.appendRow({ rank: mid });
      await writer.appendRow({ rank: max });
      await writer.close();

      const reader = await ParquetReader.openFile(testFile);

      const stats = reader.metadata!.row_groups[0].columns[0].meta_data!.statistics;

      expect(stats?.min).to.deep.equal(min);
      expect(stats?.max).to.deep.equal(max);
    });
  });

  describe('Integer Comparison', function () {
    it('correctly chooses the min and max for Int64', async function () {
      const intSchema = new ParquetSchema({
        rank: { type: 'INT64' },
      });
      const testFile = 'min-max-int.parquet';
      const writer = await ParquetWriter.openFile(intSchema, testFile);

      const min = 100n;
      const mid = 200n;
      const max = 300n;
      await writer.appendRow({ rank: min });
      await writer.appendRow({ rank: mid });
      await writer.appendRow({ rank: max });
      await writer.close();

      const reader = await ParquetReader.openFile(testFile);

      const stats = reader.metadata!.row_groups[0].columns[0].meta_data!.statistics;

      expect(stats?.min).to.deep.equal(min);
      expect(stats?.max).to.deep.equal(max);
    });
  });

  describe('String Comparison', function () {
    it('correctly chooses the min and max for UTF8', async function () {
      const intSchema = new ParquetSchema({
        rank: { type: 'UTF8' },
      });
      const testFile = 'min-max-int.parquet';
      const writer = await ParquetWriter.openFile(intSchema, testFile);

      const min = 'A';
      const mid = 'B';
      const max = 'C';
      await writer.appendRow({ rank: min });
      await writer.appendRow({ rank: mid });
      await writer.appendRow({ rank: max });
      await writer.close();

      const reader = await ParquetReader.openFile(testFile);

      const stats = reader.metadata!.row_groups[0].columns[0].meta_data!.statistics;

      expect(stats?.min).to.deep.equal(min);
      expect(stats?.max).to.deep.equal(max);
    });
  });


  describe('Timestamp Comparison', function () {
    it('correctly chooses the min and max for timestamps', async function () {
      const intSchema = new ParquetSchema({
        rank: { type: 'TIMESTAMP_MICROS' },
      });
      const testFile = 'min-max-int.parquet';
      const writer = await ParquetWriter.openFile(intSchema, testFile);

      const min = 33_000;
      const mid = 222_000;
      const max = 911_933_000;
      await writer.appendRow({ rank: min });
      await writer.appendRow({ rank: mid });
      await writer.appendRow({ rank: max });
      await writer.close();

      const reader = await ParquetReader.openFile(testFile);

      const stats = reader.metadata!.row_groups[0].columns[0].meta_data!.statistics;

      expect(+(stats?.min as unknown as Date)).to.deep.equal(33);
      expect(+(stats?.max as unknown as Date)).to.deep.equal(911_933);

    })
  })
});
