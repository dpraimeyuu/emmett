import { defineConfig } from 'tsup';

const env = process.env.NODE_ENV;

export default defineConfig({
  splitting: true,
  clean: true, // clean up the dist folder
  dts: true, // generate dts files
  format: ['esm', 'cjs'], // generate cjs and esm files
  minify: true, //env === 'production',
  bundle: true, //env === 'production',
  skipNodeModulesBundle: true,
  watch: env === 'development',
  target: 'esnext',
  outDir: 'dist', //env === 'production' ? 'dist' : 'lib',
  entry: ['src/index.ts'],
  sourcemap: true,
  outExtension: ({ format }) => ({
    js: format === 'esm' ? '.mjs' : '.js', // Use .mjs for ESM and .js for CJS
  }),
});
