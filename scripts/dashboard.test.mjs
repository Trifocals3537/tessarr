import assert from 'node:assert/strict';
import test from 'node:test';

await import('../pkg/server/assets/js/dashboard.js');

function dashboardPrototype() {
    return globalThis.TessarrTorrentDashboard.prototype;
}

function renderProviderCell(torrent) {
    const dashboard = Object.create(dashboardPrototype());
    dashboard.escapeHtml = value => String(value);
    dashboard.state = {
        selectedEntries: new Set(),
        torrents: [{
            category: '',
            info_hash: '0123456789abcdef',
            name: 'Release',
            progress: 1,
            protocol: 'torrent',
            size: 100,
            speed: 0,
            state: 'pausedUP',
            ...torrent,
        }],
    };
    dashboard.refs = {torrentsList: {innerHTML: ''}};
    dashboard.renderTorrents();
    return dashboard.refs.torrentsList.innerHTML;
}

test('dashboard displays the active failover provider', () => {
    const html = renderProviderCell({
        active_provider: 'torbox-secondary',
        debrid: 'torbox-primary',
    });
    assert.match(html, />torbox-secondary<\/span>/);
    assert.doesNotMatch(html, />torbox-primary<\/span>/);
});

test('dashboard retains the legacy provider fallback', () => {
    const html = renderProviderCell({debrid: 'realdebrid'});
    assert.match(html, />realdebrid<\/span>/);
});
