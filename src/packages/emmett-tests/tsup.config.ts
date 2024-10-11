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
  entryPoints: ['src/index.ts'],
  watch: env === 'development',
  target: 'esnext',
  outDir: 'dist', //env === 'production' ? 'dist' : 'lib',
  entry: ['src/index.ts', 'src/emmett.config.ts'],
  sourcemap: true,
  tsconfig: 'tsconfig.build.json', // workaround for https://github.com/egoist/tsup/issues/571#issuecomment-1760052931
  outExtension: ({ format }) => ({
    js: format === 'esm' ? '.mjs' : '.js', // Use .mjs for ESM and .js for CJS
  }),
});
