const fs = require('fs');
const path = require('path');
const recast = require('recast');

function createParser(babelParser) {
  return {
    parse(source) {
      return babelParser.parse(source, {
        sourceType: 'module',
        tokens: true,
        plugins: ['importAttributes'],
      });
    },
  };
}

//list of external packages that require '.js' extensions
const packagesRequiringJsExtension = [
  'protobufjs/minimal',
  //add other package paths as needed
];

function shouldAppendJsExtension(source) {
  //check if the path has an extension already
  if (path.extname(source)) {
    return false;
  }

  //check if the path is relative
  if (source.startsWith('./') || source.startsWith('../')) {
    return true;
  }

  //check if the path is in the whitelist of external packages
  return packagesRequiringJsExtension.some(pkg => source === pkg || source.startsWith(`${pkg}/`));
}


function processFile(filePath, parser) {
  const code = fs.readFileSync(filePath, 'utf8');
  const ast = recast.parse(code, {
    parser,
  });

  let modified = false;

  recast.types.visit(ast, {
    visitImportDeclaration(pathNode) {
      const source = pathNode.node.source.value;
      if (shouldAppendJsExtension(source)) {
        pathNode.node.source.value = `${source}.js`;
        modified = true;
      }
      this.traverse(pathNode);
    },
    visitExportNamedDeclaration(pathNode) {
      if (pathNode.node.source && pathNode.node.source.value) {
        const source = pathNode.node.source.value;
        if (shouldAppendJsExtension(source)) {
          pathNode.node.source.value = `${source}.js`;
          modified = true;
        }
      }
      this.traverse(pathNode);
    },
    visitExportAllDeclaration(pathNode) {
      if (pathNode.node.source && pathNode.node.source.value) {
        const source = pathNode.node.source.value;
        if (shouldAppendJsExtension(source)) {
          pathNode.node.source.value = `${source}.js`;
          modified = true;
        }
      }
      this.traverse(pathNode);
    },
  });

  if (modified) {
    const output = recast.print(ast).code;
    fs.writeFileSync(filePath, output, 'utf8');
    console.log(`Updated import/export paths in: ${filePath}`);
  }
}


function traverseDir(dir, parser) {
  fs.readdirSync(dir).forEach((file) => {
    const fullPath = path.join(dir, file);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) {
      traverseDir(fullPath, parser);
    } else if (stat.isFile() && path.extname(fullPath) === '.js') {
      processFile(fullPath, parser);
    }
  });
}

async function main() {
  const babelParser = await import('@babel/parser');
  const parser = createParser(babelParser);
  const esmDir = path.resolve(__dirname, './dist/esm');

  if (!fs.existsSync(esmDir)) {
    console.error(`Directory not found: ${esmDir}`);
    process.exit(1);
  }

  traverseDir(esmDir, parser);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
