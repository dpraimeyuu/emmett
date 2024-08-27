import { defineConfig } from 'tsup';

const env = process.env.NODE_ENV;

export default defineConfig({
  splitting: true,
  clean: true, // clean up the dist folder
  dts: true, // generate dts files
  format: ['cjs', 'esm'], // generate cjs and esm files
  minify: true, //env === 'production',
  bundle: true, //env === 'production',
  skipNodeModulesBundle: true,
  entryPoints: ['src/index.ts'],
  watch: env === 'development',
  target: 'esnext',
  outDir: 'dist', //env === 'production' ? 'dist' : 'lib',
  entry: ['src/index.ts'],
  sourcemap: true,
});
