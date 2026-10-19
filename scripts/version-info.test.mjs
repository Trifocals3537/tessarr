import assert from 'node:assert/strict';
import test from 'node:test';
import TessarrUtils from '../pkg/server/assets/js/common.js';

async function renderVersion(t, version, channel) {
    const badge = {
        children: [],
        textContent: '',
        replaceChildren() { this.children = []; this.textContent = ''; },
        appendChild(child) { this.children.push(child); },
        classList: {add() {}, remove() {}},
    };
    const previousDocument = globalThis.document;
    globalThis.document = {
        getElementById(id) { return id === 'version-badge' ? badge : null; },
        createElement(tag) { assert.equal(tag, 'a'); return {tagName: 'A'}; },
    };
    t.after(() => {
        if (previousDocument === undefined) delete globalThis.document;
        else globalThis.document = previousDocument;
    });
    const utils = Object.create(TessarrUtils.prototype);
    utils.fetcher = async endpoint => {
        assert.equal(endpoint, '/version');
        return {ok: true, json: async () => ({version, channel})};
    };
    await utils.setupVersionInfo();
    return badge;
}

test('continuous beta version links to its source commit', async t => {
    const version = '2.6.0-beta.140.5d9f4ae';
    const badge = await renderVersion(t, version, 'beta');
    assert.equal(badge.children.length, 1);
    assert.equal(badge.children[0].href, 'https://github.com/Trifocals3537/tessarr/commit/5d9f4ae');
    assert.equal(badge.children[0].textContent, version);
    assert.equal(badge.children[0].rel, 'noopener noreferrer');
});

for (const [version, channel] of [['2.6.0', 'stable'], ['2.6.0-beta.1', 'beta'], ['2.6.0-rc.1', 'rc']]) {
    test(`${channel} release version retains its release link`, async t => {
        const badge = await renderVersion(t, version, channel);
        assert.equal(badge.children.length, 1);
        assert.equal(badge.children[0].href, `https://github.com/Trifocals3537/tessarr/releases/tag/v${version}`);
        assert.equal(badge.children[0].textContent, version);
    });
}

for (const [version, channel, label] of [['ci-5d9f4aec79da', 'ci', 'ci-5d9f4aec79da'], ['', '', 'Development']]) {
    test(`${channel || 'development'} version has no release link`, async t => {
        const badge = await renderVersion(t, version, channel);
        assert.equal(badge.children.length, 0);
        assert.equal(badge.textContent, label);
    });
}
