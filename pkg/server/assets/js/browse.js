// WebDAV-style File Browser with URL-based navigation
class FileBrowser {
    constructor() {
        this.state = {
            currentPath: '/',
            currentPage: 1,
            itemsPerPage: 20,
            searchQuery: '',
            sortBy: 'name',
            sortOrder: 'asc',
            entries: [],
            total: 0,
            totalPages: 0,
            parentDir: null,
            selectedEntry: null,
            selectedEntries: new Set(),
            selectedEntryData: new Map(),
            health: new Map()
        };

        this.refs = {
            breadcrumbNav: document.getElementById('breadcrumbNav'),
            refreshBtn: document.getElementById('refreshBtn'),
            searchInput: document.getElementById('searchInput'),
            pageSizeSelect: document.getElementById('pageSizeSelect'),
            sortHeaderButtons: document.querySelectorAll('[data-sort-key]'),
            fileBrowserList: document.getElementById('fileBrowserList'),
            paginationInfo: document.getElementById('paginationInfo'),
            paginationControls: document.getElementById('paginationControls'),
            emptyState: document.getElementById('emptyState'),

            // Bulk actions
            selectAllCheckbox: document.getElementById('selectAllCheckbox'),
            bulkActionsBar: document.getElementById('bulkActionsBar'),
            selectedCount: document.getElementById('selectedCount'),
            bulkDownloadBtn: document.getElementById('bulkDownloadBtn'),
            bulkDeleteBtn: document.getElementById('bulkDeleteBtn'),
            clearSelectionBtn: document.getElementById('clearSelectionBtn'),

            // Context menu
            contextMenu: document.getElementById('contextMenu'),
            contextDownload: document.getElementById('contextDownload'),
            contextDelete: document.getElementById('contextDelete')
        };

        this.searchTimeout = null;
        this.activeLoadController = null;
        this.loadRequestSeq = 0;

        this.init();
    }

    init() {
        this.bindEvents();
        this.loadStateFromURL();
        this.loadEntries();
    }

    bindEvents() {
        // Refresh
        this.refs.refreshBtn.addEventListener('click', () => this.refresh());

        // Search with debounce
        this.refs.searchInput.addEventListener('input', (e) => {
            clearTimeout(this.searchTimeout);
            this.searchTimeout = setTimeout(() => {
                this.state.searchQuery = e.target.value;
                this.state.currentPage = 1;
                this.updateURL();
                this.refresh();
            }, 300);
        });

        // Page size
        this.refs.pageSizeSelect.addEventListener('change', (e) => {
            this.state.itemsPerPage = parseInt(e.target.value);
            this.state.currentPage = 1;
            this.updateURL();
            this.refresh();
        });

        // Sort headers
        if (this.refs.sortHeaderButtons) {
            this.refs.sortHeaderButtons.forEach(btn => {
                btn.addEventListener('click', () => {
                    this.handleSortHeaderClick(btn.dataset.sortKey);
                });
            });
        }

        // Select all checkbox
        if (this.refs.selectAllCheckbox) {
            this.refs.selectAllCheckbox.addEventListener('change', (e) => {
                this.handleSelectAll(e.target.checked);
            });
        }

        // Bulk action buttons
        if (this.refs.bulkDownloadBtn) {
            this.refs.bulkDownloadBtn.addEventListener('click', () => this.bulkDownload());
        }
        if (this.refs.bulkDeleteBtn) {
            this.refs.bulkDeleteBtn.addEventListener('click', () => this.bulkDelete());
        }
        if (this.refs.clearSelectionBtn) {
            this.refs.clearSelectionBtn.addEventListener('click', () => this.clearSelection());
        }

        // Hide context menu on click outside
        document.addEventListener('click', (e) => {
            if (!this.refs.contextMenu.contains(e.target)) {
                this.hideContextMenu();
            }
        });

        // Prevent default context menu
        document.addEventListener('contextmenu', (e) => {
            const row = e.target.closest('tr[data-entry]');
            if (row) {
                e.preventDefault();
            }
        });

        // Handle browser back/forward
        window.addEventListener('popstate', () => {
            this.loadStateFromURL();
            this.refresh();
        });

        this.refs.paginationControls.addEventListener('click', (e) => {
            const button = e.target.closest('[data-page]');
            if (!button || button.disabled) return;
            this.goToPage(Number.parseInt(button.dataset.page, 10));
        });
    }

    loadStateFromURL() {
        const params = new URLSearchParams(window.location.search);

        // Load path from URL
        this.state.currentPath = params.get('path') || '/';

        // Load page from URL
        const page = parseInt(params.get('page'));
        this.state.currentPage = page > 0 ? page : 1;

        // Load search from URL
        this.state.searchQuery = params.get('search') || '';
        if (this.refs.searchInput) {
            this.refs.searchInput.value = this.state.searchQuery;
        }

        // Load page size from URL
        const limit = parseInt(params.get('limit'));
        this.state.itemsPerPage = limit > 0 ? limit : 20;
        if (this.refs.pageSizeSelect) {
            this.refs.pageSizeSelect.value = this.state.itemsPerPage.toString();
        }

        // Load sorting from URL
        const sortBy = params.get('sort_by');
        this.state.sortBy = this.isValidSortBy(sortBy) ? sortBy : 'name';

        const sortOrder = params.get('sort_order');
        this.state.sortOrder = sortOrder === 'desc' ? 'desc' : 'asc';
        this.updateSortHeaderIndicators();
    }

    updateURL() {
        const params = new URLSearchParams();

        // Always include path
        if (this.state.currentPath !== '/') {
            params.set('path', this.state.currentPath);
        }

        // Include page if not 1
        if (this.state.currentPage > 1) {
            params.set('page', this.state.currentPage);
        }

        // Include search if present
        if (this.state.searchQuery) {
            params.set('search', this.state.searchQuery);
        }

        // Include limit if not default
        if (this.state.itemsPerPage !== 20) {
            params.set('limit', this.state.itemsPerPage);
        }

        // Include sort params if not defaults
        if (this.state.sortBy !== 'name') {
            params.set('sort_by', this.state.sortBy);
        }
        if (this.state.sortOrder !== 'asc') {
            params.set('sort_order', this.state.sortOrder);
        }

        const newURL = `${window.location.pathname}${params.toString() ? '?' + params.toString() : ''}`;
        window.history.pushState({}, '', newURL);
    }

    navigate(path) {
        this.state.currentPath = path;
        this.state.currentPage = 1;
        this.updateURL();
        this.loadEntries();
    }

    refresh() {
        this.loadEntries();
    }

    async loadEntries() {
        const requestId = ++this.loadRequestSeq;
        if (this.activeLoadController) {
            this.activeLoadController.abort();
        }
        this.activeLoadController = new AbortController();

        try {
            // Build API URL based on path depth
            const pathParts = this.state.currentPath.split('/').filter(p => p);
            let apiUrl = `${window.urlBase}api/browse`;

            if (pathParts.length === 0) {
                apiUrl += '/';
            } else if (pathParts.length === 1) {
                apiUrl += `/${encodeURIComponent(pathParts[0])}`;
            } else if (pathParts.length === 2) {
                apiUrl += `/${encodeURIComponent(pathParts[0])}/${encodeURIComponent(pathParts[1])}`;
            } else if (pathParts.length === 3) {
                apiUrl += `/${encodeURIComponent(pathParts[0])}/${encodeURIComponent(pathParts[1])}/${encodeURIComponent(pathParts[2])}`;
            }

            // Add query params
            const params = new URLSearchParams({
                page: this.state.currentPage,
                limit: this.state.itemsPerPage,
                sort_by: this.state.sortBy,
                sort_order: this.state.sortOrder
            });

            if (this.state.searchQuery) {
                params.set('search', this.state.searchQuery);
            }

            const response = await fetch(`${apiUrl}?${params}`, {signal: this.activeLoadController.signal});
            if (!response.ok) throw new Error('Failed to load directory');

            const data = await response.json();
            if (requestId !== this.loadRequestSeq) {
                return;
            }
            this.state.entries = data.entries || [];
            this.state.total = data.total || 0;
            this.state.totalPages = data.total_pages || 0;
            this.state.parentDir = data.parent_dir;

            this.updateBreadcrumbs();
            this.renderEntries();
            this.renderPagination();
            this.loadHealthForEntries();
        } catch (error) {
            if (error.name === 'AbortError') {
                return;
            }
            console.error('Error loading entries:', error);
            window.createToast('Failed to load directory', 'error');
        }
    }

    async loadHealthForEntries() {
        const names = Array.from(new Set(this.state.entries
            .filter((e) => e && e.name)
            .map((e) => e.name)));
        if (names.length === 0) return;
        const results = await Promise.all(names.map(async (name) => {
            try {
                const res = await fetch(`${window.urlBase}api/repair/health/${encodeURIComponent(name)}`);
                if (!res.ok) return [name, null];
                const state = await res.json();
                return [name, state];
            } catch {
                return [name, null];
            }
        }));
        for (const [name, state] of results) {
            if (state) this.state.health.set(name, state);
        }
        this.refreshHealthBadges();
    }

    refreshHealthBadges() {
        document.querySelectorAll('[data-health-cell]').forEach((cell) => {
            const name = cell.getAttribute('data-health-cell');
            this.renderHealthBadge(cell, this.state.health.get(name));
        });
    }

    renderHealthBadge(container, state) {
        const colors = {
            healthy: 'badge-success',
            broken: 'badge-error',
            repairing: 'badge-info',
            stale: 'badge-warning',
            unsupported: 'badge-ghost',
            unknown: 'badge-ghost',
        };
        const status = state?.status || 'unknown';
        const tooltip = state?.last_checked_at
            ? `last checked ${new Date(state.last_checked_at).toLocaleString()}`
            : 'never checked';
        const badge = document.createElement('span');
        badge.className = `badge ${colors[status] || 'badge-ghost'} badge-sm`;
        badge.title = tooltip;
        badge.textContent = status;
        container.replaceChildren(badge);
    }

    async recheckEntry(name) {
        try {
            window.createToast?.(`Rechecking ${name}…`, 'info');
            const url = `${window.urlBase}api/repair/health/${encodeURIComponent(name)}/check`;
            const res = await fetch(url, {method: 'POST'});
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(txt || `HTTP ${res.status}`);
            }
            const state = await res.json();
            this.state.health.set(name, state);
            this.refreshHealthBadges();
            window.createToast?.(`Health: ${state.status}`, state.status === 'broken' ? 'warning' : 'success');
        } catch (e) {
            console.error('Recheck failed', e);
            window.createToast?.(`Recheck failed: ${e.message}`, 'error');
        }
    }

    updateBreadcrumbs() {
        const parts = this.state.currentPath.split('/').filter(p => p);
		this.refs.breadcrumbNav.replaceChildren();

		const appendBreadcrumb = (path, label, home = false) => {
			const item = document.createElement('li');
			const link = document.createElement('a');
			link.href = path === '/'
				? `${window.urlBase}browse`
				: `${window.urlBase}browse?${new URLSearchParams({path})}`;
			link.dataset.path = path;
			if (home) {
				const icon = document.createElement('i');
				icon.className = 'bi bi-house-door';
				link.append(icon, document.createTextNode(' Home'));
			} else {
				link.textContent = label;
			}
			link.addEventListener('click', (e) => {
				e.preventDefault();
				this.navigate(path);
			});
			item.appendChild(link);
			this.refs.breadcrumbNav.appendChild(item);
		};

		appendBreadcrumb('/', 'Home', true);
        let currentPath = '';
        parts.forEach(part => {
            currentPath += '/' + part;
			let displayName = part;
			try {
				displayName = decodeURIComponent(part);
			} catch (_) {
				// Keep malformed path fragments printable instead of aborting the UI.
			}
			appendBreadcrumb(currentPath, displayName);
        });
    }

    renderEntries() {
        if (this.state.entries.length === 0) {
            this.refs.fileBrowserList.innerHTML = '';
            this.refs.emptyState.classList.remove('hidden');
            this.refs.paginationInfo.textContent = 'No items found';
            return;
        }

        this.refs.emptyState.classList.add('hidden');

        this.refs.fileBrowserList.replaceChildren();
        this.state.entries.forEach(entry => {
            const entryId = entry.info_hash || entry.path;
            const isChecked = this.state.selectedEntries.has(entryId);
			const row = document.createElement('tr');
			row.className = 'group hover:bg-base-200 transition-colors';
			row.dataset.entryId = String(entryId || '');
			row.addEventListener('contextmenu', (event) => this.showContextMenu(event, entry));

			const selectionCell = document.createElement('td');
			selectionCell.addEventListener('click', (event) => event.stopPropagation());
			const selectionLabel = document.createElement('label');
			selectionLabel.className = 'cursor-pointer';
			const checkbox = document.createElement('input');
			checkbox.type = 'checkbox';
			checkbox.className = 'checkbox checkbox-sm checkbox-primary entry-checkbox';
			checkbox.dataset.entryId = String(entryId || '');
			checkbox.checked = isChecked;
			checkbox.addEventListener('change', () => this.handleEntrySelect(entryId, checkbox.checked, entry));
			selectionLabel.appendChild(checkbox);
			selectionCell.appendChild(selectionLabel);

			const iconCell = document.createElement('td');
			const icon = document.createElement('i');
			icon.className = entry.is_dir
				? 'bi bi-folder-fill text-warning text-lg transition-colors group-hover:text-warning-content'
				: 'bi bi-file-earmark text-info transition-colors group-hover:text-info-content';
			iconCell.appendChild(icon);

			const nameCell = document.createElement('td');
			nameCell.className = 'cursor-pointer hover:text-primary transition-colors';
			nameCell.addEventListener('click', () => this.handleEntryClick(entry.path, entry.is_dir, entry.name));
			const name = document.createElement('span');
			name.className = 'font-medium';
			name.textContent = entry.name || '';
			nameCell.appendChild(name);

			const sizeCell = document.createElement('td');
			sizeCell.textContent = entry.size <= 0 ? '-' : this.formatSize(entry.size);
			const modifiedCell = document.createElement('td');
			modifiedCell.className = 'text-xs text-base-content/70';
			modifiedCell.textContent = entry.mod_time || '-';
			const providerCell = document.createElement('td');
			providerCell.textContent = entry.active_debrid || '-';

			const healthCell = document.createElement('td');
			healthCell.dataset.healthCell = entry.name || '';
			this.renderHealthBadge(healthCell, this.state.health.get(entry.name));

			const actionsCell = document.createElement('td');
			actionsCell.addEventListener('click', (event) => event.stopPropagation());
			actionsCell.appendChild(this.createEntryActions(entry));

			row.append(selectionCell, iconCell, nameCell, sizeCell, modifiedCell, providerCell, healthCell, actionsCell);
			this.refs.fileBrowserList.appendChild(row);
		});

        this.updateSelectionUI();
    }

	createEntryActions(entry) {
		const dropdown = document.createElement('div');
		dropdown.className = 'dropdown dropdown-end';
		const trigger = document.createElement('button');
		trigger.type = 'button';
		trigger.className = 'btn btn-ghost btn-xs';
		trigger.setAttribute('aria-label', `Actions for ${entry.name || 'entry'}`);
		const triggerIcon = document.createElement('i');
		triggerIcon.className = 'bi bi-three-dots-vertical';
		trigger.appendChild(triggerIcon);

		const menu = document.createElement('ul');
		menu.className = 'dropdown-content menu p-2 shadow bg-base-200 rounded-box w-52 z-50';
		menu.tabIndex = 0;
		const appendAction = (label, iconClass, action, className = '') => {
			const item = document.createElement('li');
			const link = document.createElement('button');
			link.type = 'button';
			link.className = className;
			const icon = document.createElement('i');
			icon.className = iconClass;
			link.append(icon, document.createTextNode(` ${label}`));
			link.addEventListener('click', action);
			item.appendChild(link);
			menu.appendChild(item);
		};

		if (!entry.is_dir) {
			appendAction('Download', 'bi bi-download', () => this.downloadFile(entry.path, entry.name));
		}
		appendAction('Recheck health', 'bi bi-search-heart', () => this.recheckEntry(entry.name));
		if (entry.can_delete) {
			appendAction('Delete', 'bi bi-trash', () => this.deleteTorrent(entry.info_hash, entry.name), 'text-error');
		}
		dropdown.append(trigger, menu);
		return dropdown;
	}

    renderPagination() {
        const start = (this.state.currentPage - 1) * this.state.itemsPerPage + 1;
        const end = Math.min(start + this.state.itemsPerPage - 1, this.state.total);

        this.refs.paginationInfo.textContent = this.state.total > 0
            ? `Showing ${start}-${end} of ${this.state.total} items`
            : 'No items';

        if (this.state.totalPages <= 1) {
            this.refs.paginationControls.innerHTML = '';
            return;
        }

		this.refs.paginationControls.replaceChildren();
		const appendPageButton = (page, label, options = {}) => {
			const button = document.createElement('button');
			button.type = 'button';
			button.className = `join-item btn btn-sm${options.active ? ' btn-active' : ''}${options.disabled ? ' btn-disabled' : ''}`;
			button.disabled = Boolean(options.disabled);
			if (!options.disabled) button.dataset.page = String(page);
			if (options.icon) {
				const icon = document.createElement('i');
				icon.className = options.icon;
				button.appendChild(icon);
			} else {
				button.textContent = label;
			}
			this.refs.paginationControls.appendChild(button);
		};

		appendPageButton(this.state.currentPage - 1, '', {
			disabled: this.state.currentPage === 1,
			icon: 'bi bi-chevron-left'
		});

        // Smart pagination: show first, last, current, and nearby pages
        for (let i = 1; i <= this.state.totalPages; i++) {
            if (i === 1 || i === this.state.totalPages ||
                (i >= this.state.currentPage - 2 && i <= this.state.currentPage + 2)) {
				appendPageButton(i, String(i), {active: i === this.state.currentPage});
            } else if (i === this.state.currentPage - 3 || i === this.state.currentPage + 3) {
				appendPageButton(0, '...', {disabled: true});
            }
        }

		appendPageButton(this.state.currentPage + 1, '', {
			disabled: this.state.currentPage === this.state.totalPages,
			icon: 'bi bi-chevron-right'
		});
    }

    goToPage(page) {
        if (!Number.isInteger(page) || page < 1 || page > this.state.totalPages) return;
        this.state.currentPage = page;
        this.updateURL();
        this.refresh();
    }

    isValidSortBy(sortBy) {
        return ['name', 'size', 'mod_time', 'active_debrid'].includes(sortBy);
    }

    handleSortHeaderClick(sortBy) {
        if (!this.isValidSortBy(sortBy)) {
            return;
        }

        if (this.state.sortBy === sortBy) {
            this.state.sortOrder = this.state.sortOrder === 'asc' ? 'desc' : 'asc';
        } else {
            this.state.sortBy = sortBy;
            // Fresh column defaults: text asc, numeric/time desc.
            this.state.sortOrder = (sortBy === 'size' || sortBy === 'mod_time') ? 'desc' : 'asc';
        }

        this.state.currentPage = 1;
        this.updateSortHeaderIndicators();
        this.updateURL();
        this.refresh();
    }

    updateSortHeaderIndicators() {
        const sortKeys = ['name', 'size', 'mod_time', 'active_debrid'];
        sortKeys.forEach(key => {
            const indicator = document.getElementById(`sortIndicator-${key}`);
            if (!indicator) return;

            if (this.state.sortBy === key) {
                indicator.className = this.state.sortOrder === 'asc' ? 'bi bi-sort-up text-xs' : 'bi bi-sort-down text-xs';
            } else {
                indicator.className = 'bi bi-arrow-down-up text-xs';
            }
        });
    }

    handleEntryClick(path, isDir, name) {
        if (isDir) {
            this.navigate(path);
        } else {
            this.downloadFile(path, name);
        }
    }

    showContextMenu(event, entry) {
        event.preventDefault();
        event.stopPropagation();

        // Position context menu
        this.refs.contextMenu.style.left = event.pageX + 'px';
        this.refs.contextMenu.style.top = event.pageY + 'px';
        this.refs.contextMenu.classList.remove('hidden');

        // Show/hide appropriate menu items
        if (!entry.is_dir) {
            this.refs.contextDownload.classList.remove('hidden');
            this.refs.contextDownload.onclick = () => this.downloadFile(entry.path, entry.name);
        } else {
            this.refs.contextDownload.classList.add('hidden');
        }

        if (entry.can_delete) {
            this.refs.contextDelete.classList.remove('hidden');
            this.refs.contextDelete.onclick = () => this.deleteTorrent(entry.info_hash, entry.name);
        } else {
            this.refs.contextDelete.classList.add('hidden');
        }

        this.state.selectedEntry = entry;
    }

    hideContextMenu() {
        this.refs.contextMenu.classList.add('hidden');
    }

    downloadFile(path, fileName) {
        this.hideContextMenu();

        // Extract torrent and file names from path
        const pathParts = path.split('/').filter(p => p);
        if (pathParts.length < 3) return;

        const torrentName = pathParts[pathParts.length - 2];
        const file = pathParts[pathParts.length - 1];

        const downloadUrl = `${window.urlBase}api/browse/download/${encodeURIComponent(torrentName)}/${encodeURIComponent(file)}`;
        window.open(downloadUrl, '_blank');
    }

    async deleteTorrent(infoHash, name) {
        this.hideContextMenu();

        if (!confirm(`Delete "${name}"?\n\nThis will remove the item from the system.`)) {
            return;
        }

        try {
			const response = await fetch(`${window.urlBase}api/browse/torrents/${encodeURIComponent(infoHash)}`, {
                method: 'DELETE'
            });

            if (!response.ok) throw new Error('Failed to delete entry');

            window.createToast('Item deleted successfully', 'success');
            this.refresh();
        } catch (error) {
            console.error('Error deleting item:', error);
            window.createToast('Failed to delete item', 'error');
        }
    }

    // Utility methods
    formatSize(bytes) {
        if (!bytes || bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    escapeAttr(text) {
        if (typeof text !== 'string') {
            text = JSON.stringify(text);
        }
        return text.replace(/'/g, '&#39;').replace(/"/g, '&quot;');
    }

    escapeJs(text) {
        if (typeof text !== 'string') {
            text = String(text);
        }
        return text.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
    }

    // Multi-select methods
    handleEntrySelect(entryId, checked, entry) {
        if (checked) {
            this.state.selectedEntries.add(entryId);
            if (entry) {
                this.state.selectedEntryData.set(entryId, entry);
            }
        } else {
            this.state.selectedEntries.delete(entryId);
            this.state.selectedEntryData.delete(entryId);
        }
        this.updateSelectionUI();
    }

    handleSelectAll(checked) {
        if (checked) {
            this.state.entries.forEach(entry => {
                const entryId = entry.info_hash || entry.path;
                this.state.selectedEntries.add(entryId);
                this.state.selectedEntryData.set(entryId, entry);
            });
        } else {
            this.state.entries.forEach(entry => {
                const entryId = entry.info_hash || entry.path;
                this.state.selectedEntries.delete(entryId);
                this.state.selectedEntryData.delete(entryId);
            });
        }

        // Update all checkboxes
        document.querySelectorAll('.entry-checkbox').forEach(checkbox => {
            checkbox.checked = checked;
        });

        this.updateSelectionUI();
    }

    updateSelectionUI() {
        const selectedCount = this.state.selectedEntries.size;

        // Update count
        if (this.refs.selectedCount) {
            this.refs.selectedCount.textContent = selectedCount;
        }

        // Show/hide bulk actions bar
        if (this.refs.bulkActionsBar) {
            if (selectedCount > 0) {
                this.refs.bulkActionsBar.classList.remove('hidden');
            } else {
                this.refs.bulkActionsBar.classList.add('hidden');
            }
        }

        // Update select all checkbox state
        if (this.refs.selectAllCheckbox) {
            const allSelected = this.state.entries.length > 0 &&
                this.state.entries.every(entry => {
                    const entryId = entry.info_hash || entry.path;
                    return this.state.selectedEntries.has(entryId);
                });
            this.refs.selectAllCheckbox.checked = allSelected;
            this.refs.selectAllCheckbox.indeterminate = selectedCount > 0 && !allSelected;
        }
    }

    clearSelection() {
        this.state.selectedEntries.clear();
        this.state.selectedEntryData.clear();
        document.querySelectorAll('.entry-checkbox').forEach(checkbox => {
            checkbox.checked = false;
        });
        this.updateSelectionUI();
    }

    bulkDownload() {
        const selectedEntries = this.getSelectedEntries();
        const files = selectedEntries.filter(e => !e.is_dir);

        if (files.length === 0) {
            window.createToast('No files selected for download', 'warning');
            return;
        }

        files.forEach(entry => {
            this.downloadFile(entry.path, entry.name);
        });

        window.createToast(`Downloading ${files.length} file(s)`, 'success');
    }

    async bulkDelete() {
        const selectedEntries = this.getSelectedEntries();
        const torrents = selectedEntries.filter(e => e.can_delete && e.info_hash);

        if (torrents.length === 0) {
            window.createToast('No items selected for deletion', 'warning');
            return;
        }

        const names = torrents.map(t => t.name).join('\n');
        if (!confirm(`Delete ${torrents.length} torrent(s)?\n\n${names}\n\nThis will remove the items from the management system.`)) {
            return;
        }

        const ids = [...new Set(torrents.map(t => t.info_hash).filter(Boolean))];
        try {
            const response = await fetch(`${window.urlBase}api/browse/torrents/batch`, {
                method: 'DELETE',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ids})
            });
            if (!response.ok) throw new Error('Failed to delete selected items');
            window.createToast(`Deleted ${ids.length} torrent(s)`, 'success');
            this.clearSelection();
            this.refresh();
        } catch (error) {
            console.error('Error deleting selected items:', error);
            window.createToast('Failed to delete selected items', 'error');
        }
    }

    getSelectedEntries() {
        return Array.from(this.state.selectedEntryData.values()).filter(Boolean);
    }

    async bulkRecheck() {
        const selected = this.getSelectedEntries();
        if (!selected.length) {
            window.createToast('No items selected', 'warning');
            return;
        }
        for (const entry of selected) {
            if (entry?.name) await this.recheckEntry(entry.name);
        }
    }
}
