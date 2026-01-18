#!/usr/bin/env node
/**
 * Copy the native Neon addon after build.
 *
 * This script copies the built .node file to the expected location.
 */

import { existsSync, copyFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const projectRoot = join(__dirname, '..');

// Possible source locations for the built addon
const sources = [
  join(projectRoot, 'native', 'target', 'release', 'libpuffgres_neon.dylib'),
  join(projectRoot, 'native', 'target', 'release', 'libpuffgres_neon.so'),
  join(projectRoot, 'native', 'target', 'release', 'puffgres_neon.dll'),
  join(projectRoot, 'native', 'target', 'debug', 'libpuffgres_neon.dylib'),
  join(projectRoot, 'native', 'target', 'debug', 'libpuffgres_neon.so'),
  join(projectRoot, 'native', 'target', 'debug', 'puffgres_neon.dll'),
  // Neon puts the .node file in index.node by default
  join(projectRoot, 'native', 'index.node'),
];

// Destination
const dest = join(projectRoot, 'native', 'puffgres.node');

function copyNative() {
  for (const source of sources) {
    if (existsSync(source)) {
      console.log(`Copying native addon from ${source}`);
      copyFileSync(source, dest);
      console.log(`  -> ${dest}`);
      return;
    }
  }

  console.log('Note: No native addon found to copy.');
  console.log('Run "npm run build:native" to build the native addon first.');
}

copyNative();
