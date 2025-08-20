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

function memoryUsage() {
  const usage = process.memoryUsage();
  return {
    heap: (usage.heapUsed / 1024 / 1024).toFixed(1),
    rss: (usage.rss / 1024 / 1024).toFixed(1),
  };
}

async function benchmarkWrite(rows, columns, description, iterations = 100) {
  console.log(`\n--- ${description} ---`);
  console.log(`Dataset: ${rows} rows × ${columns} columns`);
  console.log(`Running ${iterations} iterations for averaging...`);

  const schema = createSchemaForColumns(columns);
  const data = await createLargeDataset(rows, columns);

  const results = [];

  for (let iteration = 0; iteration < iterations; iteration++) {
    const filename = `/tmp/benchmark_${columns}cols_${rows}rows_${iteration}.parquet`;

    // Clean up any existing file
    try {
      fs.unlinkSync(filename);
    } catch (e) {}

    if (global.gc) global.gc();

    const startMem = memoryUsage();
    const startTime = process.hrtime.bigint();

    try {
      const writer = await ParquetWriter.openFile(schema, filename);

      let maxHeap = parseFloat(startMem.heap);
      let maxRSS = parseFloat(startMem.rss);

      for (let i = 0; i < data.length; i++) {
        await writer.appendRow(data[i]);

        // Sample memory every 200 records
        if (i % 200 === 0) {
          const currentMem = memoryUsage();
          maxHeap = Math.max(maxHeap, parseFloat(currentMem.heap));
          maxRSS = Math.max(maxRSS, parseFloat(currentMem.rss));
        }
      }

      await writer.close();

      const endTime = process.hrtime.bigint();
      const endMem = memoryUsage();

      const duration = Number(endTime - startTime) / 1000000;
      const fileSize = fs.statSync(filename).size / (1024 * 1024);
      const throughput = (rows * columns) / (duration / 1000);

      results.push({
        success: true,
        duration,
        maxHeap,
        maxRSS,
        finalHeap: parseFloat(endMem.heap),
        finalRSS: parseFloat(endMem.rss),
        fileSize,
        throughput,
      });

      // Clean up
      try {
        fs.unlinkSync(filename);
      } catch (e) {}

      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (error) {
      const endTime = process.hrtime.bigint();
      const duration = Number(endTime - startTime) / 1000000;

      results.push({
        success: false,
        error: error.message,
        duration,
      });

      try {
        fs.unlinkSync(filename);
      } catch (e) {}
    }
  }

  // Calculate averages
  const successful = results.filter((r) => r.success);
  if (successful.length === 0) {
    console.log(`❌ ALL ${iterations} ITERATIONS FAILED!`);
    return { duration: 0, peakMemoryMB: 0, fileSize: 0 };
  }

  const avgDuration = successful.reduce((sum, r) => sum + r.duration, 0) / successful.length;
  const avgMaxHeap = successful.reduce((sum, r) => sum + r.maxHeap, 0) / successful.length;
  const avgMaxRSS = successful.reduce((sum, r) => sum + r.maxRSS, 0) / successful.length;
  const avgFileSize = successful.reduce((sum, r) => sum + r.fileSize, 0) / successful.length;
  const avgThroughput = successful.reduce((sum, r) => sum + r.throughput, 0) / successful.length;

  const durationStdDev = Math.sqrt(
    successful.reduce((sum, r) => sum + Math.pow(r.duration - avgDuration, 2), 0) / successful.length
  );
  const heapStdDev = Math.sqrt(
    successful.reduce((sum, r) => sum + Math.pow(r.maxHeap - avgMaxHeap, 2), 0) / successful.length
  );
  const rssStdDev = Math.sqrt(
    successful.reduce((sum, r) => sum + Math.pow(r.maxRSS - avgMaxRSS, 2), 0) / successful.length
  );

  console.log(`✅ SUCCESS (${successful.length}/${iterations} runs)`);
  console.log(`Time: ${avgDuration.toFixed(0)}ms (±${durationStdDev.toFixed(1)}ms)`);
  console.log(
    `Peak Memory: ${avgMaxHeap.toFixed(1)}MB heap (±${heapStdDev.toFixed(1)}MB), ${avgMaxRSS.toFixed(1)}MB RSS (±${rssStdDev.toFixed(1)}MB)`
  );
  console.log(`File Size: ${avgFileSize.toFixed(1)}MB`);
  console.log(`Throughput: ${Math.round(avgThroughput).toLocaleString()} values/sec`);

  return { duration: avgDuration, peakMemoryMB: avgMaxHeap, fileSize: avgFileSize };
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
    { rows: 1000, columns: 200, desc: 'Very wide table (1K rows × 200 cols)' },
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
  results.forEach((result) => {
    const valuesPerSec = ((result.rows * result.columns) / (result.duration / 1000)).toFixed(0);
    console.log(`${result.desc}:`);
    console.log(
      `  ${result.duration.toFixed(0)}ms | ${result.peakMemoryMB.toFixed(1)}MB heap | ${valuesPerSec} vals/sec`
    );
  });

  // Analysis
  console.log('\n🔍 OPTIMIZATION ANALYSIS');
  console.log('========================');

  const wideResults = results.filter((r) => r.columns >= 50);
  const narrowResults = results.filter((r) => r.columns <= 10);

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
