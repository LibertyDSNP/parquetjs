import 'regenerator-runtime/runtime';
import 'core-js/stable';
const coreImportPromise = import('./parquet').catch((e) => console.error('Error importing `parquet.js`:', e));

export const core = coreImportPromise;
