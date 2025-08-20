const { ParquetWriter, ParquetSchema } = require('./dist/parquet.js');
const fs = require('fs');

async function createLargeDataset(rows, columns) {
  const data = [];
  for (let i = 0; i < rows; i++) {
    const record = {};
    for (let j = 0; j < columns; j++) {
      record[`col_${j}`] = Math.random() * 1000;
    }
    data.push(record);
  }
  return data;
}

function createSchemaForColumns(columns) {
  const fields = {};
  for (let i = 0; i < columns; i++) {
    fields[`col_${i}`] = { type: 'DOUBLE' };
  }
  return new ParquetSchema(fields);
}

async function benchmarkWrite(rows, columns, description) {
  console.log(`\n--- ${description} ---`);
  console.log(`Dataset: ${rows} rows × ${columns} columns`);
  
  const schema = createSchemaForColumns(columns);
  const data = await createLargeDataset(rows, columns);
  const filename = `/tmp/benchmark_${columns}cols_${rows}rows.parquet`;
  
  // Clean up any existing file
  try { fs.unlinkSync(filename); } catch (e) {}
  
  const startTime = process.hrtime.bigint();
  const startMemory = process.memoryUsage();
  
  const writer = await ParquetWriter.openFile(schema, filename);
  
  for (const record of data) {
    await writer.appendRow(record);
  }
  
  await writer.close();
  
  const endTime = process.hrtime.bigint();
  const endMemory = process.memoryUsage();
  
  const duration = Number(endTime - startTime) / 1000000; // Convert to milliseconds
  const peakMemoryMB = Math.max(startMemory.heapUsed, endMemory.heapUsed) / (1024 * 1024);
  const fileSize = fs.statSync(filename).size / (1024 * 1024); // MB
  
  console.log(`Time: ${duration.toFixed(0)}ms`);
  console.log(`Peak Memory: ${peakMemoryMB.toFixed(1)}MB`);
  console.log(`File Size: ${fileSize.toFixed(1)}MB`);
  console.log(`Throughput: ${((rows * columns) / (duration / 1000)).toFixed(0)} values/sec`);
  
  // Clean up
  try { fs.unlinkSync(filename); } catch (e) {}
  
  return { duration, peakMemoryMB, fileSize };
}

async function runBenchmarks() {
  console.log('🚀 Benchmarking Memory Optimizations');
  console.log('=====================================');
  
  // Test different scenarios to see where optimizations help most
  const scenarios = [
    { rows: 1000, columns: 10, desc: 'Small dataset (1K rows × 10 cols)' },
    { rows: 1000, columns: 50, desc: 'Medium columns (1K rows × 50 cols)' },
    { rows: 1000, columns: 100, desc: 'Many columns (1K rows × 100 cols)' },
    { rows: 5000, columns: 50, desc: 'Large dataset (5K rows × 50 cols)' },
    { rows: 1000, columns: 200, desc: 'Very wide table (1K rows × 200 cols)' }
  ];
  
  const results = [];
  
  for (const scenario of scenarios) {
    try {
      const result = await benchmarkWrite(scenario.rows, scenario.columns, scenario.desc);
      results.push({ ...scenario, ...result });
      
      // Force garbage collection between tests
      if (global.gc) {
        global.gc();
      }
    } catch (error) {
      console.error(`❌ Error in ${scenario.desc}:`, error.message);
    }
  }
  
  console.log('\n📊 SUMMARY');
  console.log('==========');
  results.forEach(result => {
    const valuesPerSec = ((result.rows * result.columns) / (result.duration / 1000)).toFixed(0);
    console.log(`${result.desc}:`);
    console.log(`  ${result.duration.toFixed(0)}ms | ${result.peakMemoryMB.toFixed(1)}MB | ${valuesPerSec} vals/sec`);
  });
  
  // Analysis
  console.log('\n🔍 OPTIMIZATION ANALYSIS');
  console.log('========================');
  
  const wideResults = results.filter(r => r.columns >= 50);
  const narrowResults = results.filter(r => r.columns <= 10);
  
  if (wideResults.length > 0 && narrowResults.length > 0) {
    const avgWideTime = wideResults.reduce((sum, r) => sum + r.duration, 0) / wideResults.length;
    const avgNarrowTime = narrowResults.reduce((sum, r) => sum + r.duration, 0) / narrowResults.length;
    
    console.log(`Average time for narrow tables (≤10 cols): ${avgNarrowTime.toFixed(0)}ms`);
    console.log(`Average time for wide tables (≥50 cols): ${avgWideTime.toFixed(0)}ms`);
    console.log(`The O(n²) → O(n) buffer optimization should show bigger improvements in wide tables.`);
  }
  
  console.log('\n✅ Benchmark completed!');
}

// Run with garbage collection enabled if possible
if (process.argv.includes('--expose-gc')) {
  console.log('🗑️  Garbage collection enabled for more accurate memory measurements');
}

runBenchmarks().catch(console.error);