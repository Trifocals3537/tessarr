#!/usr/bin/env node

const fs = require('node:fs/promises');
const path = require('node:path');
const {createHash, randomUUID} = require('node:crypto');

// Hash the original upstream bytes before rewriting any CSS paths. See
// docs/codeql-developer-tools-review.md for provenance and the update procedure.
const assets = Object.freeze([
    {
        url: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/bootstrap-icons.css',
        path: 'pkg/server/assets/css/bootstrap-icons.css',
        sha256: 'bd6146337f91518bdf30381cd35ab6db9ea4056572bfb7d8c7d6a3bd642c7fba',
        css: true,
    },
    {
        url: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/fonts/bootstrap-icons.woff',
        path: 'pkg/server/assets/build/fonts/bootstrap-icons.woff',
        sha256: 'd0346eea5a5ca9998be3c506fc655f0cbfccb42deb5fc6df39815425af867bb5',
    },
    {
        url: 'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/fonts/bootstrap-icons.woff2',
        path: 'pkg/server/assets/build/fonts/bootstrap-icons.woff2',
        sha256: '5b2dd4d4f81cd1f52a50b0833ea12c8f63f2c4ae4d2c5a799fcc741feb2ea40f',
    },
    {
        url: 'https://code.jquery.com/jquery-3.7.1.min.js',
        path: 'pkg/server/assets/build/js/jquery-3.7.1.min.js',
        sha256: 'fc9a93dd241f6b045cbff0481cf4e1901becd0e12fb45166a8f17f95823f0b1a',
    }
].map(Object.freeze));

const maxAssetBytes = 1024 * 1024;

async function fetchVerifiedAsset(asset, fetchImpl = fetch, timeoutMs = 30_000) {
    // The versioned endpoints need no redirects. Fail closed if that changes.
    const response = await fetchImpl(asset.url, {
        redirect: 'error',
        signal: AbortSignal.timeout(timeoutMs),
    });
    if (response.status !== 200 || !response.body) {
        await response.body?.cancel();
        throw new Error(`Asset request failed: HTTP ${response.status}`);
    }

    const chunks = [];
    let size = 0;
    for await (const chunk of response.body) {
        size += chunk.length;
        if (size > maxAssetBytes) {
            throw new Error('Asset exceeds the 1 MiB download limit');
        }
        chunks.push(chunk);
    }
    const bytes = Buffer.concat(chunks);
    if (createHash('sha256').update(bytes).digest('hex') !== asset.sha256) {
        throw new Error(`SHA-256 mismatch for ${path.basename(asset.path)}`);
    }
    return bytes;
}

async function downloadAssets({root = path.resolve(__dirname, '..'), fetchImpl = fetch, timeoutMs = 30_000} = {}) {
    // Validate the entire set before changing any existing asset. Network
    // failures or mismatches must not leave a mixture of trusted/untrusted files.
    const verified = [];
    for (const asset of assets) {
        const bytes = await fetchVerifiedAsset(asset, fetchImpl, timeoutMs);
        verified.push({asset, bytes});
    }
    for (const {asset, bytes} of verified) {
        const destination = path.join(root, asset.path);
        const content = asset.css
            ? bytes.toString('utf8').replace(/url\("\.\/fonts\//g, 'url("../fonts/')
            : bytes;
        await fs.mkdir(path.dirname(destination), {recursive: true});
        const temporary = `${destination}.${randomUUID()}.tmp`;
        try {
            await fs.writeFile(temporary, content, {flag: 'wx'});
            await fs.rename(temporary, destination);
        } finally {
            await fs.rm(temporary, {force: true});
        }
        console.log(`Verified ${path.basename(destination)} (${bytes.length} bytes)`);
    }
}

if (require.main === module) {
    downloadAssets().catch(error => {
        console.error('Asset download failed:', error.message);
        process.exitCode = 1;
    });
}

module.exports = {assets, fetchVerifiedAsset, downloadAssets};
