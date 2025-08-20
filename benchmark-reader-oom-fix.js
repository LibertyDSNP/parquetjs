const { ParquetWriter, ParquetReader, ParquetSchema } = require('./dist/parquet.js');
const fs = require('fs');

function memoryUsage() {
  const usage = process.memoryUsage();
  return {
    heap: (usage.heapUsed / 1024 / 1024).toFixed(1),
    rss: (usage.rss / 1024 / 1024).toFixed(1),
  };
}

async function createTestFile(columns, rows, filename) {
  console.log(`📝 Creating test file: ${rows} rows × ${columns} columns...`);

  // Create schema
  const fields = {};
  for (let i = 0; i < columns; i++) {
    fields[`col_${i}`] = { type: 'DOUBLE' };
  }
  const schema = new ParquetSchema(fields);

  // Create and write data
  const writer = await ParquetWriter.openFile(schema, filename);

  for (let i = 0; i < rows; i++) {
    const record = {};
    for (let j = 0; j < columns; j++) {
      record[`col_${j}`] = Math.random() * 1000;
    }
    await writer.appendRow(record);
  }

  await writer.close();

  const fileSize = (fs.statSync(filename).size / 1024 / 1024).toFixed(2);
  console.log(`   File created: ${fileSize}MB`);
  return fileSize;
}

async function benchmarkReader(filename, columns, rows, description, iterations = 100) {
  console.log(`\n📊 ${description}`);
  console.log(`   File: ${filename} (${rows} rows × ${columns} cols)`);
  console.log(`   Running ${iterations} iterations for averaging...`);

  const results = [];

  for (let iteration = 0; iteration < iterations; iteration++) {
    if (global.gc) global.gc(); // Force GC before each test

    const startMem = memoryUsage();
    const startTime = process.hrtime.bigint();

    try {
      const reader = await ParquetReader.openFile(filename);

      let maxHeap = parseFloat(startMem.heap);
      let maxRSS = parseFloat(startMem.rss);
      let recordCount = 0;

      const cursor = reader.getCursor();

      // Read data and track memory during reading
      for (let record = await cursor.next(); record; record = await cursor.next()) {
        recordCount++;

        // Sample memory every 100 records
        if (recordCount % 100 === 0) {
          const currentMem = memoryUsage();
          maxHeap = Math.max(maxHeap, parseFloat(currentMem.heap));
          maxRSS = Math.max(maxRSS, parseFloat(currentMem.rss));
        }
      }

      await reader.close();

      const endTime = process.hrtime.bigint();
      const endMem = memoryUsage();

      const duration = Number(endTime - startTime) / 1000000; // ms
      const throughput = Math.round((recordCount * columns) / (duration / 1000));

      results.push({
        success: true,
        duration,
        maxHeap,
        maxRSS,
        finalHeap: parseFloat(endMem.heap),
        finalRSS: parseFloat(endMem.rss),
        recordCount,
        throughput,
        columns,
        rows,
      });

      // Brief pause between iterations
      await new Promise((resolve) => setTimeout(resolve, 100));
    } catch (error) {
      const endTime = process.hrtime.bigint();
      const duration = Number(endTime - startTime) / 1000000;

      results.push({
        success: false,
        error: error.message,
        duration,
        columns,
        rows,
      });
    }
  }

  // Calculate averages for successful runs
  const successful = results.filter((r) => r.success);
  if (successful.length === 0) {
    console.log(`   ❌ ALL ${iterations} ITERATIONS FAILED!`);
    return results[0]; // Return first failure
  }

  const avgDuration = successful.reduce((sum, r) => sum + r.duration, 0) / successful.length;
  const avgMaxHeap = successful.reduce((sum, r) => sum + r.maxHeap, 0) / successful.length;
  const avgMaxRSS = successful.reduce((sum, r) => sum + r.maxRSS, 0) / successful.length;
  const avgFinalHeap = successful.reduce((sum, r) => sum + r.finalHeap, 0) / successful.length;
  const avgFinalRSS = successful.reduce((sum, r) => sum + r.finalRSS, 0) / successful.length;
  const avgThroughput = successful.reduce((sum, r) => sum + r.throughput, 0) / successful.length;

  // Calculate standard deviations for key metrics
  const durationStdDev = Math.sqrt(
    successful.reduce((sum, r) => sum + Math.pow(r.duration - avgDuration, 2), 0) / successful.length
  );
  const heapStdDev = Math.sqrt(
    successful.reduce((sum, r) => sum + Math.pow(r.maxHeap - avgMaxHeap, 2), 0) / successful.length
  );
  const rssStdDev = Math.sqrt(
    successful.reduce((sum, r) => sum + Math.pow(r.maxRSS - avgMaxRSS, 2), 0) / successful.length
  );

  console.log(`   ✅ SUCCESS (${successful.length}/${iterations} runs)`);
  console.log(`   Duration: ${avgDuration.toFixed(1)}ms (±${durationStdDev.toFixed(1)}ms)`);
  console.log(`   Records read: ${successful[0].recordCount}`);
  console.log(
    `   Peak memory: ${avgMaxHeap.toFixed(1)}MB heap (±${heapStdDev.toFixed(1)}MB), ${avgMaxRSS.toFixed(1)}MB RSS (±${rssStdDev.toFixed(1)}MB)`
  );
  console.log(`   Final memory: ${avgFinalHeap.toFixed(1)}MB heap, ${avgFinalRSS.toFixed(1)}MB RSS`);
  console.log(`   Throughput: ${Math.round(avgThroughput).toLocaleString()} values/sec`);
  console.log(`   Memory efficiency: ${(avgMaxHeap / columns).toFixed(3)}MB heap per column`);

  return {
    success: true,
    duration: avgDuration,
    maxHeap: avgMaxHeap,
    maxRSS: avgMaxRSS,
    finalHeap: avgFinalHeap,
    finalRSS: avgFinalRSS,
    recordCount: successful[0].recordCount,
    throughput: avgThroughput,
    columns,
    rows,
    iterations: successful.length,
    durationStdDev,
    heapStdDev,
    rssStdDev,
  };
}

async function runOOMBenchmark() {
  console.log('🔬 READER OOM FIX BENCHMARK');
  console.log('============================');
  console.log('Testing the pre-allocated array optimization for wide tables');
  console.log('Previous issue: OOM with 300+ column files\n');

  const testCases = [
    { columns: 50, rows: 1000, desc: 'Baseline: 50 columns (should always work)' },
    { columns: 100, rows: 1000, desc: 'Medium width: 100 columns' },
    { columns: 200, rows: 1000, desc: 'Wide table: 200 columns' },
    { columns: 300, rows: 1000, desc: 'Very wide table: 300 columns (OOM test)' },
    { columns: 500, rows: 500, desc: 'Extreme width: 500 columns (ultimate test)' },
  ];

  const results = [];

  for (const testCase of testCases) {
    const filename = `/tmp/oom_test_${testCase.columns}cols_${testCase.rows}rows.parquet`;

    try {
      // Clean up any existing file
      try {
        fs.unlinkSync(filename);
      } catch (e) {}

      // Create test file
      await createTestFile(testCase.columns, testCase.rows, filename);

      // Benchmark reading
      const result = await benchmarkReader(filename, testCase.columns, testCase.rows, testCase.desc);

      results.push(result);

      // Clean up
      try {
        fs.unlinkSync(filename);
      } catch (e) {}

      // Force GC between tests
      if (global.gc) global.gc();
    } catch (error) {
      console.log(`❌ Test case failed: ${error.message}`);
      results.push({
        success: false,
        error: error.message,
        columns: testCase.columns,
        rows: testCase.rows,
      });
    }
  }

  // Summary
  console.log('\n📈 BENCHMARK SUMMARY');
  console.log('====================');

  const successful = results.filter((r) => r.success);
  const failed = results.filter((r) => !r.success);

  if (successful.length > 0) {
    console.log(`✅ Successful tests: ${successful.length}/${results.length}`);
    console.log('\nPerformance breakdown:');

    successful.forEach((result) => {
      const memoryPerColumn = (result.maxHeap / result.columns).toFixed(2);
      console.log(
        `  ${result.columns} cols: ${result.duration}ms | ${result.maxHeap}MB peak | ${memoryPerColumn}MB/col | ${result.throughput.toLocaleString()} vals/sec`
      );
    });

    // Check if 300-column test passed (the main OOM issue)
    const oom300Test = results.find((r) => r.columns === 300);
    if (oom300Test && oom300Test.success) {
      console.log('\n🎉 OOM FIX VERIFIED: 300-column files now work!');
      console.log(`   300 columns used ${oom300Test.maxHeap}MB peak memory`);
      console.log(`   Memory per column: ${(oom300Test.maxHeap / 300).toFixed(2)}MB`);
    }
  }

  if (failed.length > 0) {
    console.log(`\n❌ Failed tests: ${failed.length}`);
    failed.forEach((result) => {
      console.log(`  ${result.columns} cols: ${result.error}`);
    });
  }

  // Memory scaling analysis
  if (successful.length >= 3) {
    console.log('\n🔍 MEMORY SCALING ANALYSIS:');
    const scaling = successful.map((r) => ({ cols: r.columns, mem: r.maxHeap }));
    scaling.sort((a, b) => a.cols - b.cols);

    console.log('Column count vs Peak memory:');
    scaling.forEach((s) => console.log(`  ${s.cols} columns: ${s.mem}MB`));

    // Check if scaling is linear (good) vs quadratic (bad)
    if (scaling.length >= 2) {
      const first = scaling[0];
      const last = scaling[scaling.length - 1];
      const memoryGrowthRatio = last.mem / first.mem;
      const columnGrowthRatio = last.cols / first.cols;
      const scalingFactor = memoryGrowthRatio / columnGrowthRatio;

      console.log(`Memory scaling factor: ${scalingFactor.toFixed(2)} (1.0 = linear, >2.0 = problematic)`);

      if (scalingFactor < 1.5) {
        console.log('✅ Memory scales nearly linearly with column count - optimization working!');
      } else if (scalingFactor < 2.5) {
        console.log('⚠️  Memory scaling is acceptable but could be better');
      } else {
        console.log('❌ Memory scaling is problematic - may still have issues');
      }
    }
  }

  console.log('\n✅ OOM Fix Benchmark Completed!');
}

// Run with garbage collection for accurate memory measurements
if (process.argv.includes('--expose-gc')) {
  console.log('🗑️  Garbage collection enabled for accurate memory measurements');
} else {
  console.log('💡 Run with --expose-gc for more accurate memory measurements');
}

runOOMBenchmark().catch(console.error);
