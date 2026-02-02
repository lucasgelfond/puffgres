#!/usr/bin/env node
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import Turbopuffer from '@turbopuffer/turbopuffer';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Simple .env loader - looks for .env* files in current dir and parent dirs
function loadEnvFile(envPath) {
  if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, 'utf8');
    content.split('\n').forEach(line => {
      const match = line.match(/^\s*([^#=]+?)\s*=\s*(.*)$/);
      if (match && !process.env[match[1]]) {
        process.env[match[1]] = match[2].replace(/^["']|["']$/g, '');
      }
    });
    console.log(`Loaded env from ${envPath}`);
  }
}

function loadEnv() {
  let dir = process.cwd();
  while (dir !== path.dirname(dir)) {
    const files = fs.readdirSync(dir).filter(f => f.startsWith('.env'));
    for (const file of files) {
      loadEnvFile(path.join(dir, file));
    }
    dir = path.dirname(dir);
  }
}
loadEnv();

const PORT = process.env.PORT || 3333;
const API_KEY = process.env.TURBOPUFFER_API_KEY;
const REGION = process.env.TURBOPUFFER_REGION || 'aws-us-east-1';

if (!API_KEY) {
  console.error('Error: TURBOPUFFER_API_KEY environment variable is required');
  process.exit(1);
}

// Initialize Turbopuffer client
const tpuf = new Turbopuffer({
  apiKey: API_KEY,
  region: REGION,
});

const server = http.createServer(async (req, res) => {
  // Serve index.html
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    const htmlPath = path.join(__dirname, 'index.html');
    fs.readFile(htmlPath, (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Error loading page');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
    return;
  }

  // API proxy endpoint - query documents
  if (req.method === 'POST' && req.url === '/api/query') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      try {
        const { namespace, limit } = JSON.parse(body);

        if (!namespace) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'namespace is required' }));
          return;
        }

        const ns = tpuf.namespace(namespace);
        const result = await ns.query({
          rank_by: ['id', 'asc'],
          top_k: limit || 10,
          include_attributes: true,
        });

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        console.error('Error:', err.message);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // List namespaces
  if (req.method === 'GET' && req.url.startsWith('/api/namespaces') && !req.url.includes('/api/namespaces/')) {
    try {
      const url = new URL(req.url, `http://localhost:${PORT}`);
      const cursor = url.searchParams.get('cursor');
      const prefix = url.searchParams.get('prefix');

      const params = {};
      if (prefix) params.prefix = prefix;
      if (cursor) params.cursor = cursor;

      // Use the SDK's namespaces method - returns an async iterator/page
      const page = await tpuf.namespaces(params);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        namespaces: page.data || [],
        next_cursor: page.nextPageInfo()?.cursor || null,
      }));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Delete namespace
  if (req.method === 'DELETE' && req.url.startsWith('/api/namespaces/')) {
    const namespace = decodeURIComponent(req.url.replace('/api/namespaces/', ''));
    try {
      const ns = tpuf.namespace(namespace);
      await ns.deleteAll();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true }));
    } catch (err) {
      console.error('Error:', err.message);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // 404
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

server.listen(PORT, () => {
  console.log(`Debug UI running at http://localhost:${PORT}`);
});
