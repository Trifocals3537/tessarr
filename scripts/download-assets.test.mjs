import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdtemp, mkdir, readFile, readdir, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import test from 'node:test';
import downloader from './download-assets.js';

const {assets, fetchVerifiedAsset, downloadAssets} = downloader;
const sample = Buffer.from('trusted test asset\n');
const testAsset = {url: 'https://example.invalid/asset', path: 'asset', sha256: createHash('sha256').update(sample).digest('hex')};

test('only exact pinned bytes are accepted; redirects and deadlines are explicit', async () => {
    const bytes = await fetchVerifiedAsset(testAsset, async (url, options) => {
        assert.equal(url, testAsset.url);
        assert.equal(options.redirect, 'error');
        assert.ok(options.signal instanceof AbortSignal);
        return new Response(sample);
    });
    assert.deepEqual(bytes, sample);
    await assert.rejects(fetchVerifiedAsset(testAsset, async () => new Response('tampered')), /SHA-256 mismatch/);
});

for (const status of [206, 301, 302, 404, 500]) {
    test(`HTTP ${status} is rejected without reading its content`, async () => {
        let cancelled = false;
        const body = new ReadableStream({cancel() {cancelled = true;}});
        await assert.rejects(fetchVerifiedAsset(testAsset, async () => new Response(body, {status})), /Asset request failed/);
        assert.equal(cancelled, true);
    });
}

test('oversized and interrupted response streams fail closed', async () => {
    let cancelled = false;
    const largeBody = new ReadableStream({
        start(controller) {controller.enqueue(new Uint8Array(1024 * 1024 + 1));},
        cancel() {cancelled = true;},
    });
    await assert.rejects(fetchVerifiedAsset(testAsset, async () => new Response(largeBody)), /download limit/);
    assert.equal(cancelled, true);
    const brokenBody = new ReadableStream({start(controller) {controller.error(new Error('connection interrupted'));}});
    await assert.rejects(fetchVerifiedAsset(testAsset, async () => new Response(brokenBody)), /connection interrupted/);
});

test('a stalled download observes the abort deadline', async () => {
    // Keep this mocked request alive until the real AbortSignal timeout fires.
    await assert.rejects(fetchVerifiedAsset(testAsset, async (_url, {signal}) => {
        await delay(1000, undefined, {signal});
    }, 10), {name: 'AbortError'});
});

test('an untrusted download never overwrites an existing asset or creates new files', async t => {
    const root = await mkdtemp(path.join(tmpdir(), 'tessarr-assets-'));
    t.after(() => rm(root, {recursive: true, force: true}));
    const destination = path.join(root, assets[0].path);
    await mkdir(path.dirname(destination), {recursive: true});
    await writeFile(destination, 'previous verified content');
    await assert.rejects(downloadAssets({root, fetchImpl: async () => new Response('untrusted CSS')}), /SHA-256 mismatch/);
    assert.equal(await readFile(destination, 'utf8'), 'previous verified content');
    assert.deepEqual(await readdir(path.dirname(destination)), [path.basename(destination)]);
    await assert.rejects(readFile(path.join(root, assets[1].path)), {code: 'ENOENT'});
});

test('integrity pins cover all four fixed HTTPS destinations and are immutable', () => {
    assert.equal(assets.length, 4);
    assert.ok(Object.isFrozen(assets));
    for (const asset of assets) {
        assert.ok(Object.isFrozen(asset));
        assert.equal(new URL(asset.url).protocol, 'https:');
        assert.match(asset.sha256, /^[a-f0-9]{64}$/);
        assert.ok(asset.path.startsWith('pkg/server/assets/'));
        assert.ok(!asset.path.includes('..'));
    }
});
