// Patches the napi loader to use scoped packages `@triton-one` instead of `./` packages
// We need this since we're publishing the platform specific NAPI binaries in the `@triton-one` scope on npm
// and NAPI CLI doesn't have an option to use scoped packages
// Run this only during release builds, otherwise it'll break local development and
// look for packages npmjs.com instead of local build
// This file is purposefully written in JS and not TS to avoid having a TS compiler during builds from package.json
const fs = require('fs');
const path = require('path');

const loaderFile = path.resolve(
  __dirname,
  './index.js'
);

const patchedLoaderFile = loaderFile.replace(".js","-patched.js");

// Match require('./yellowstone-grpc-napi.something.node')
const NODE_REQUIRE_REGEX =
  /require\(['"]\.\/(yellowstone-grpc-napi(?:[.\w-]+)?)\.node['"]\)/g;

let source = fs.readFileSync(loaderFile, 'utf8');

const patched = source.replace(NODE_REQUIRE_REGEX, (_, basename) => {
  // Convert internal filename to scoped package name
  //
  // Example:
  //   ./yellowstone-grpc-napi.darwin-arm64
  // becomes:
  //   @triton-one/yellowstone-grpc-napi-darwin-arm64
  //
  const pkg = `@triton-one/${basename.replace(/\./g, '-')}`;
  return `require('${pkg}')`;
});

fs.writeFileSync(patchedLoaderFile, patched);

console.log(`patched NAPI loader ${patchedLoaderFile} to use scoped packages`);
