// Thanks to https://github.com/httptoolkit/brotli-wasm/issues/8#issuecomment-1746768478
import init, * as brotliWasm from '../../node_modules/brotli-wasm/pkg.web/brotli_wasm.js';
import wasmUrl from '../../node_modules/brotli-wasm/pkg.web/brotli_wasm_bg.wasm';

const brotliPromise = init(wasmUrl).then(() => brotliWasm);

// We chunk to protect the wasm memory
const OUTPUT_SIZE = 131_072;

function mergeUint8Arrays(arrs: Uint8Array[], totalLength: number): Uint8Array {
  const output = new Uint8Array(totalLength);
  let priorLength = 0;
  for (const arr of arrs) {
    output.set(arr, priorLength);
    priorLength += arr.length;
  }
  console.log('output Length', output.length);
  return output;
}

export async function compress(input: Uint8Array): Promise<Uint8Array> {
  console.log('Compress Input Length: ', input.length);
  // Get a stream for your input:
  const inputStream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(input);
      controller.close();
    },
  });

  // Create a stream to incrementally compress the data as it streams:
  const brotli = await brotliPromise;
  const compressStream = new brotli.CompressStream();
  const compressionStream = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      let resultCode;
      let inputOffset = 0;

      // Compress this chunk, producing up to OUTPUT_SIZE output bytes at a time, until the
      // entire input has been compressed.

      do {
        const inputChunk = chunk.slice(inputOffset);
        const result = compressStream.compress(inputChunk, OUTPUT_SIZE);
        controller.enqueue(result.buf);
        resultCode = result.code;
        inputOffset += result.input_offset;
      } while (resultCode === brotli.BrotliStreamResultCode.NeedsMoreOutput);
      if (resultCode !== brotli.BrotliStreamResultCode.NeedsMoreInput) {
        controller.error(`Brotli compression failed when transforming with code ${resultCode}`);
      }
    },
    flush(controller) {
      // Once the chunks are finished, flush any remaining data (again in repeated fixed-output
      // chunks) to finish the stream:
      let resultCode;
      do {
        const result = compressStream.compress(undefined, OUTPUT_SIZE);
        controller.enqueue(result.buf);
        resultCode = result.code;
      } while (resultCode === brotli.BrotliStreamResultCode.NeedsMoreOutput);
      if (resultCode !== brotli.BrotliStreamResultCode.ResultSuccess) {
        controller.error(`Brotli compression failed when flushing with code ${resultCode}`);
      }
      controller.terminate();
    },
  });

  const outputs: Uint8Array[] = [];
  let outputLength = 0;
  const outputStream = new WritableStream<Uint8Array>({
    write(chunk) {
      outputs.push(chunk);
      outputLength += chunk.length;
    },
  });

  await inputStream.pipeThrough(compressionStream).pipeTo(outputStream);

  return mergeUint8Arrays(outputs, outputLength);
}

export async function inflate(input: Uint8Array): Promise<Uint8Array> {
  console.log('Inflate Input Length: ', input.length);
  // Get a stream for your input:
  const inputStream = new ReadableStream({
    start(controller) {
      controller.enqueue(input);
      controller.close();
    },
  });

  const brotli = await brotliPromise;
  const decompressStream = new brotli.DecompressStream();
  const decompressionStream = new TransformStream({
    transform(chunk, controller) {
      let resultCode;
      let inputOffset = 0;

      // Decompress this chunk, producing up to OUTPUT_SIZE output bytes at a time, until the
      // entire input has been decompressed.

      do {
        const inputChunk = chunk.slice(inputOffset);
        const result = decompressStream.decompress(inputChunk, OUTPUT_SIZE);
        controller.enqueue(result.buf);
        resultCode = result.code;
        inputOffset += result.input_offset;
      } while (resultCode === brotli.BrotliStreamResultCode.NeedsMoreOutput);
      if (
        resultCode !== brotli.BrotliStreamResultCode.NeedsMoreInput &&
        resultCode !== brotli.BrotliStreamResultCode.ResultSuccess
      ) {
        controller.error(`Brotli decompression failed with code ${resultCode}`);
      }
    },
    flush(controller) {
      controller.terminate();
    },
  });
  const outputs: Uint8Array[] = [];
  let outputLength = 0;
  const outputStream = new WritableStream<Uint8Array>({
    write(chunk) {
      outputs.push(chunk);
      outputLength += chunk.length;
    },
  });

  await inputStream.pipeThrough(decompressionStream).pipeTo(outputStream);

  return mergeUint8Arrays(outputs, outputLength);
}
