const { ParquetWriter, ParquetReader, ParquetSchema } = require('./dist/parquet.js');
const fs = require('fs');

function memoryUsage() {
  const usage = process.memoryUsage();
  return {
    heap: (usage.heapUsed / 1024 / 1024).toFixed(1),
    rss: (usage.rss / 1024 / 1024).toFixed(1)
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

async function benchmarkReader(filename, columns, rows, description) {
  console.log(`\n📊 ${description}`);
  console.log(`   File: ${filename} (${rows} rows × ${columns} cols)`);
  
  if (global.gc) global.gc(); // Force GC before test
  
  const startMem = memoryUsage();
  const startTime = process.hrtime.bigint();
  
  console.log(`   Starting memory: ${startMem.heap}MB heap, ${startMem.rss}MB RSS`);
  
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
        
        // Log memory every 1000 records for very wide tables
        if (columns >= 200 && recordCount % 1000 === 0) {
          console.log(`     Record ${recordCount}: ${currentMem.heap}MB heap`);
        }
      }
    }
    
    await reader.close();
    
    const endTime = process.hrtime.bigint();
    const endMem = memoryUsage();
    
    const duration = Number(endTime - startTime) / 1000000; // ms
    const throughput = Math.round((recordCount * columns) / (duration / 1000));
    
    console.log(`   ✅ SUCCESS!`);
    console.log(`   Duration: ${duration.toFixed(0)}ms`);
    console.log(`   Records read: ${recordCount}`);
    console.log(`   Peak memory: ${maxHeap}MB heap, ${maxRSS}MB RSS`);
    console.log(`   Final memory: ${endMem.heap}MB heap, ${endMem.rss}MB RSS`);
    console.log(`   Throughput: ${throughput.toLocaleString()} values/sec`);
    console.log(`   Memory efficiency: Peak heap was ${maxHeap}MB for ${columns} columns`);
    
    return { 
      success: true, 
      duration, 
      maxHeap, 
      maxRSS, 
      recordCount, 
      throughput,
      columns,
      rows 
    };
    
  } catch (error) {
    const endTime = process.hrtime.bigint();
    const duration = Number(endTime - startTime) / 1000000;
    
    console.log(`   ❌ FAILED: ${error.message}`);
    console.log(`   Failed after: ${duration.toFixed(0)}ms`);
    
    return { 
      success: false, 
      error: error.message, 
      duration, 
      columns, 
      rows 
    };
  }
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
    { columns: 500, rows: 500, desc: 'Extreme width: 500 columns (ultimate test)' }
  ];
  
  const results = [];
  
  for (const testCase of testCases) {
    const filename = `/tmp/oom_test_${testCase.columns}cols_${testCase.rows}rows.parquet`;
    
    try {
      // Clean up any existing file
      try { fs.unlinkSync(filename); } catch (e) {}
      
      // Create test file
      await createTestFile(testCase.columns, testCase.rows, filename);
      
      // Benchmark reading
      const result = await benchmarkReader(
        filename, 
        testCase.columns, 
        testCase.rows, 
        testCase.desc
      );
      
      results.push(result);
      
      // Clean up
      try { fs.unlinkSync(filename); } catch (e) {}
      
      // Force GC between tests
      if (global.gc) global.gc();
      
    } catch (error) {
      console.log(`❌ Test case failed: ${error.message}`);
      results.push({ 
        success: false, 
        error: error.message, 
        columns: testCase.columns, 
        rows: testCase.rows 
      });
    }
  }
  
  // Summary
  console.log('\n📈 BENCHMARK SUMMARY');
  console.log('====================');
  
  const successful = results.filter(r => r.success);
  const failed = results.filter(r => !r.success);
  
  if (successful.length > 0) {
    console.log(`✅ Successful tests: ${successful.length}/${results.length}`);
    console.log('\nPerformance breakdown:');
    
    successful.forEach(result => {
      const memoryPerColumn = (result.maxHeap / result.columns).toFixed(2);
      console.log(`  ${result.columns} cols: ${result.duration}ms | ${result.maxHeap}MB peak | ${memoryPerColumn}MB/col | ${result.throughput.toLocaleString()} vals/sec`);
    });
    
    // Check if 300-column test passed (the main OOM issue)
    const oom300Test = results.find(r => r.columns === 300);
    if (oom300Test && oom300Test.success) {
      console.log('\n🎉 OOM FIX VERIFIED: 300-column files now work!');
      console.log(`   300 columns used ${oom300Test.maxHeap}MB peak memory`);
      console.log(`   Memory per column: ${(oom300Test.maxHeap / 300).toFixed(2)}MB`);
    }
  }
  
  if (failed.length > 0) {
    console.log(`\n❌ Failed tests: ${failed.length}`);
    failed.forEach(result => {
      console.log(`  ${result.columns} cols: ${result.error}`);
    });
  }
  
  // Memory scaling analysis
  if (successful.length >= 3) {
    console.log('\n🔍 MEMORY SCALING ANALYSIS:');
    const scaling = successful.map(r => ({ cols: r.columns, mem: r.maxHeap }));
    scaling.sort((a, b) => a.cols - b.cols);
    
    console.log('Column count vs Peak memory:');
    scaling.forEach(s => console.log(`  ${s.cols} columns: ${s.mem}MB`));
    
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