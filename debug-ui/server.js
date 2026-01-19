#!/usr/bin/env node
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

// Simple .env loader - looks for .env in current dir or parent dirs
function loadEnv() {
  let dir = process.cwd();
  while (dir !== path.dirname(dir)) {
    const envPath = path.join(dir, '.env');
    if (fs.existsSync(envPath)) {
      const content = fs.readFileSync(envPath, 'utf8');
      content.split('\n').forEach(line => {
        const match = line.match(/^\s*([^#=]+?)\s*=\s*(.*)$/);
        if (match && !process.env[match[1]]) {
          process.env[match[1]] = match[2].replace(/^["']|["']$/g, '');
        }
      });
      console.log(`Loaded .env from ${envPath}`);
      return;
    }
    dir = path.dirname(dir);
  }
}
loadEnv();

const PORT = process.env.PORT || 3333;
const API_KEY = process.env.TURBOPUFFER_API_KEY;

if (!API_KEY) {
  console.error('Error: TURBOPUFFER_API_KEY environment variable is required');
  process.exit(1);
}

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

  // API proxy endpoint
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

        const tpufResponse = await queryTurbopuffer(namespace, limit || 10);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(tpufResponse));
      } catch (err) {
        console.error('Error:', err.message);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // 404
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

function queryTurbopuffer(namespace, limit) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify({
      rank_by: ['id', 'asc'],
      top_k: limit,
      include_attributes: true
    });

    const options = {
      hostname: 'api.turbopuffer.com',
      port: 443,
      path: `/v2/namespaces/${encodeURIComponent(namespace)}/query`,
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (res.statusCode !== 200) {
            reject(new Error(parsed.error || parsed.message || `Turbopuffer returned ${res.statusCode}`));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error(`Invalid response from Turbopuffer: ${data.substring(0, 200)}`));
        }
      });
    });

    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

server.listen(PORT, () => {
  console.log(`Debug UI running at http://localhost:${PORT}`);
});
