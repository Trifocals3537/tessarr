import assert from 'node:assert/strict';
import test from 'node:test';
import {webcrypto} from 'node:crypto';

globalThis.window = {API: '/prefix/api', confirm: () => true};
await import('../pkg/server/assets/js/repair.js');

class Element {
    children = [];
    textContent = '';
    disabled = false;
    append(...nodes) { this.children.push(...nodes); }
    replaceChildren(...nodes) { this.children = nodes; }
    addEventListener(name, fn) { this[name] = fn; }
    set innerHTML(value) { throw new Error('Recovery data must be rendered as text'); }
}
function fixture() {
    const elements = new Map(['cleanupWorkList', 'cleanupStatus', 'cleanupHistory'].map(id => [id, new Element()]));
    globalThis.document = {getElementById: id => elements.get(id), createElement: () => new Element()};
    const manager = Object.create(window.RepairManager.prototype);
    manager.api = '/prefix/api';
    manager.toasts = [];
    manager.toast = message => manager.toasts.push(message);
    return {manager, elements};
}

test('cleanup recovery renders hostile names and disables active claims', async () => {
    const {manager, elements} = fixture();
    manager.fetchJSON = async () => ({total: 1, active: [{key: 'hash', name: '<img onerror=alert(1)>', state: 'running', attempts: 2, saved_path: '<script>unsafe</script>'}], history: []});
    await manager.loadCleanup();
    const row = elements.get('cleanupWorkList').children[0];
    assert.equal(row.children[0].textContent, '<img onerror=alert(1)>');
    assert.equal(row.children[2].textContent, '<script>unsafe</script>');
    assert.equal(row.children[3].disabled, true);
    assert.equal(row.children[4].disabled, true);
});

test('operator action sends immutable identity/revision and handles stale views', async () => {
    const {manager, elements} = fixture();
    window.confirm = () => true;
    const item = {key: 'hash/encoded', incarnation: 'saved-incarnation', revision: 7, state: 'blocked'};
    manager.fetchJSON = async () => ({total: 1, active: [item], history: []});
    let sent;
    globalThis.fetch = async (url, options) => { sent = {url, options}; return {ok: false, status: 409, text: async () => 'Cleanup changed; refresh'}; };
    assert.equal(typeof webcrypto.getRandomValues, 'function');
    await manager.resolveCleanup(item, 'reconcile', new Element());
    assert.equal(sent.url, '/prefix/api/cleanup/workflows/hash%2Fencoded/resolve');
    const body = JSON.parse(sent.options.body);
    assert.equal(body.incarnation, item.incarnation);
    assert.equal(body.revision, 7);
    assert.equal(body.action, 'reconcile');
    assert.match(body.request_id, /^[0-9a-f-]{36}$/);
    assert.deepEqual(Object.keys(body).sort(), ['action', 'incarnation', 'request_id', 'revision']);
    assert.ok(manager.toasts.includes('Cleanup changed; refresh'));
    assert.equal(elements.get('cleanupWorkList').children[0].children[3].disabled, false);
});

test('declined operator confirmation performs no request', async () => {
    const {manager} = fixture();
    window.confirm = () => false;
    globalThis.fetch = async () => { throw new Error('unexpected mutation'); };
    const button = new Element();
    await manager.resolveCleanup({key: 'hash'}, 'retry', button);
    assert.equal(button.disabled, false);
});
