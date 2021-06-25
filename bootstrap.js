const coreImportPromise = import('./parquet').catch(e => console.error('Error importing `parquet.js`:', e))

export const core = coreImportPromise;
