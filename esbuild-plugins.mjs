import path from 'node:path';
/**
 * this plugin resolves to a browser version of compression.ts that
 * does not include LZO comprssion.
 */
export const compressionBrowserPlugin = {
  name: 'compressionBrowser',
  setup(build) {
    build.onResolve({ filter: /^\.\/compression$/ }, (args) => {
      return { path: path.join(args.resolveDir, args.path.replace('compression', 'browser/compression.ts')) };
    });
  },
};
