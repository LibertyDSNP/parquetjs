'use strict';
const chai = require('chai');
const assert = chai.assert;
const { ParquetReader } = require('../lib/reader');

describe('INT96 timestamp handling', function () {
  this.timeout(30000); // Increase timeout for URL fetching

  const testUrl =
    'https://aws-public-blockchain.s3.us-east-2.amazonaws.com/v1.0/eth/traces/date%3D2016-05-22/part-00000-54f4b70c-db10-479c-a117-e3cc760a7e26-c000.snappy.parquet';

  it('should handle INT96 values as numbers by default', async function () {
    const reader = await ParquetReader.openUrl(testUrl);
    const cursor = reader.getCursor();

    // Read the first row
    const row = await cursor.next();

    // Find INT96 columns (if any)
    const schema = reader.getSchema();
    const fields = schema.fields;

    // Check if there are any INT96 columns and verify they're numbers
    let foundInt96 = false;
    for (const fieldName in fields) {
      const field = fields[fieldName];
      if (field.primitiveType === 'INT96') {
        foundInt96 = true;
        assert.isNumber(row[fieldName], `Expected ${fieldName} to be a number`);
      }
    }

    // If no INT96 columns were found, log a message
    if (!foundInt96) {
      console.log('No INT96 columns found in the test file');
    }

    await reader.close();
  });

  it('should convert INT96 values to timestamps when option is enabled', async function () {
    const reader = await ParquetReader.openUrl(testUrl, { treatInt96AsTimestamp: true });
    const cursor = reader.getCursor();

    // Read the first row
    const row = await cursor.next();

    // Find INT96 columns (if any)
    const schema = reader.getSchema();
    const fields = schema.fields;

    // Check if there are any INT96 columns and verify they're Date objects
    let foundInt96 = false;
    for (const fieldName in fields) {
      const field = fields[fieldName];
      if (field.primitiveType === 'INT96') {
        foundInt96 = true;
        assert.instanceOf(row[fieldName], Date, `Expected ${fieldName} to be a Date object`);
        console.log(`${fieldName} timestamp:`, row[fieldName]);
      }
    }

    // If no INT96 columns were found, log a message
    if (!foundInt96) {
      console.log('No INT96 columns found in the test file');
    }

    await reader.close();
  });
});
