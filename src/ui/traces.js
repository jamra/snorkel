/**
 * Trace Viewer for OpenTelemetry spans
 */

class TraceViewer {
    constructor(container) {
        this.container = container;
        this.traces = [];
        this.selectedTrace = null;
        this.spans = [];
    }

    /**
     * Search for traces matching criteria
     */
    async searchTraces(options = {}) {
        const {
            service = '',
            operation = '',
            minDuration = 0,
            maxDuration = null,
            status = '',
            limit = 100
        } = options;

        // Build SQL query
        let conditions = [];
        if (service) {
            conditions.push(`service_name = '${service}'`);
        }
        if (operation) {
            conditions.push(`span_name LIKE '%${operation}%'`);
        }
        if (minDuration > 0) {
            conditions.push(`duration_ms >= ${minDuration}`);
        }
        if (maxDuration) {
            conditions.push(`duration_ms <= ${maxDuration}`);
        }
        if (status === 'error') {
            conditions.push(`status_code = 'ERROR'`);
        }

        // Find root spans (no parent) to get trace summaries
        conditions.push(`parent_span_id = ''`);

        const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
        const sql = `
            SELECT trace_id, service_name, span_name, duration_ms, start_time, status_code
            FROM otel_traces
            ${where}
            ORDER BY start_time DESC
            LIMIT ${limit}
        `;

        try {
            const response = await fetch('/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql })
            });

            if (!response.ok) throw new Error('Query failed');

            const result = await response.json();
            this.traces = this.processTraceList(result);
            this.renderTraceList();
            return this.traces;
        } catch (error) {
            console.error('Failed to search traces:', error);
            this.showError('Failed to search traces: ' + error.message);
            return [];
        }
    }

    /**
     * Process query result into trace list
     */
    processTraceList(result) {
        if (!result.rows) return [];

        const cols = result.columns;
        const traceIdIdx = cols.indexOf('trace_id');
        const serviceIdx = cols.indexOf('service_name');
        const spanNameIdx = cols.indexOf('span_name');
        const durationIdx = cols.indexOf('duration_ms');
        const startTimeIdx = cols.indexOf('start_time');
        const statusIdx = cols.indexOf('status_code');

        return result.rows.map(row => ({
            traceId: row[traceIdIdx],
            service: row[serviceIdx],
            operation: row[spanNameIdx],
            duration: row[durationIdx],
            startTime: row[startTimeIdx],
            hasError: row[statusIdx] === 'ERROR'
        }));
    }

    /**
     * Load all spans for a specific trace
     */
    async loadTrace(traceId) {
        const sql = `
            SELECT trace_id, span_id, parent_span_id, service_name, span_name,
                   span_kind, start_time, end_time, duration_ms, status_code, status_message
            FROM otel_traces
            WHERE trace_id = '${traceId}'
            ORDER BY start_time ASC
        `;

        try {
            const response = await fetch('/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql })
            });

            if (!response.ok) throw new Error('Query failed');

            const result = await response.json();
            this.spans = this.processSpans(result);
            this.selectedTrace = traceId;
            this.renderTraceDetail();
            return this.spans;
        } catch (error) {
            console.error('Failed to load trace:', error);
            this.showError('Failed to load trace: ' + error.message);
            return [];
        }
    }

    /**
     * Process query result into span list
     */
    processSpans(result) {
        if (!result.rows) return [];

        const cols = result.columns;
        return result.rows.map(row => {
            const span = {};
            cols.forEach((col, idx) => {
                span[col] = row[idx];
            });
            return span;
        });
    }

    /**
     * Build span tree from flat list
     */
    buildSpanTree(spans) {
        const spanMap = new Map();
        const roots = [];

        // First pass: create map
        spans.forEach(span => {
            spanMap.set(span.span_id, { ...span, children: [] });
        });

        // Second pass: build tree
        spans.forEach(span => {
            const node = spanMap.get(span.span_id);
            if (span.parent_span_id && spanMap.has(span.parent_span_id)) {
                spanMap.get(span.parent_span_id).children.push(node);
            } else {
                roots.push(node);
            }
        });

        return roots;
    }

    /**
     * Render the trace list view
     */
    renderTraceList() {
        const html = `
            <div class="trace-list">
                <div class="trace-search">
                    <input type="text" id="trace-service" placeholder="Service name">
                    <input type="text" id="trace-operation" placeholder="Operation">
                    <input type="number" id="trace-min-duration" placeholder="Min duration (ms)">
                    <select id="trace-status">
                        <option value="">All statuses</option>
                        <option value="error">Errors only</option>
                    </select>
                    <button id="trace-search-btn">Search</button>
                </div>
                <table class="trace-table">
                    <thead>
                        <tr>
                            <th>Service</th>
                            <th>Operation</th>
                            <th>Duration</th>
                            <th>Time</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${this.traces.map(t => `
                            <tr class="trace-row ${t.hasError ? 'error' : ''}" data-trace-id="${t.traceId}">
                                <td>${this.escapeHtml(t.service)}</td>
                                <td>${this.escapeHtml(t.operation)}</td>
                                <td>${t.duration}ms</td>
                                <td>${this.formatTime(t.startTime)}</td>
                                <td>${t.hasError ? '<span class="error-badge">ERROR</span>' : 'OK'}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;

        this.container.innerHTML = html;
        this.bindTraceListEvents();
    }

    /**
     * Render the trace detail view (waterfall)
     */
    renderTraceDetail() {
        const tree = this.buildSpanTree(this.spans);
        if (tree.length === 0) {
            this.showError('No spans found for trace');
            return;
        }

        // Find trace time range
        const minTime = Math.min(...this.spans.map(s => s.start_time));
        const maxTime = Math.max(...this.spans.map(s => s.end_time));
        const totalDuration = maxTime - minTime;

        const html = `
            <div class="trace-detail">
                <div class="trace-header">
                    <button id="back-to-list">Back to List</button>
                    <h3>Trace: ${this.selectedTrace.substring(0, 16)}...</h3>
                    <span class="trace-duration">Total: ${totalDuration}ms</span>
                </div>
                <div class="trace-timeline">
                    <div class="timeline-header">
                        <div class="timeline-labels">
                            ${this.renderTimeLabels(totalDuration)}
                        </div>
                    </div>
                    <div class="span-list">
                        ${this.renderSpanTree(tree, minTime, totalDuration, 0)}
                    </div>
                </div>
                <div id="span-detail" class="span-detail"></div>
            </div>
        `;

        this.container.innerHTML = html;
        this.bindTraceDetailEvents();
    }

    /**
     * Render time labels for the timeline
     */
    renderTimeLabels(totalDuration) {
        const labels = [];
        const steps = 5;
        for (let i = 0; i <= steps; i++) {
            const ms = Math.round((totalDuration * i) / steps);
            labels.push(`<span class="time-label">${ms}ms</span>`);
        }
        return labels.join('');
    }

    /**
     * Render span tree recursively
     */
    renderSpanTree(nodes, minTime, totalDuration, depth) {
        return nodes.map(node => {
            const left = ((node.start_time - minTime) / totalDuration) * 100;
            const width = Math.max(0.5, (node.duration_ms / totalDuration) * 100);
            const hasError = node.status_code === 'ERROR';

            return `
                <div class="span-row" data-span-id="${node.span_id}" style="padding-left: ${depth * 20}px">
                    <div class="span-info">
                        <span class="span-service">${this.escapeHtml(node.service_name)}</span>
                        <span class="span-name">${this.escapeHtml(node.span_name)}</span>
                    </div>
                    <div class="span-bar-container">
                        <div class="span-bar ${hasError ? 'error' : ''} ${node.span_kind?.toLowerCase() || ''}"
                             style="left: ${left}%; width: ${width}%">
                            <span class="span-duration">${node.duration_ms}ms</span>
                        </div>
                    </div>
                </div>
                ${node.children.length > 0 ? this.renderSpanTree(node.children, minTime, totalDuration, depth + 1) : ''}
            `;
        }).join('');
    }

    /**
     * Show span details in sidebar
     */
    showSpanDetail(spanId) {
        const span = this.spans.find(s => s.span_id === spanId);
        if (!span) return;

        const detailEl = document.getElementById('span-detail');
        if (!detailEl) return;

        detailEl.innerHTML = `
            <h4>Span Details</h4>
            <div class="detail-section">
                <label>Service</label>
                <span>${this.escapeHtml(span.service_name)}</span>
            </div>
            <div class="detail-section">
                <label>Operation</label>
                <span>${this.escapeHtml(span.span_name)}</span>
            </div>
            <div class="detail-section">
                <label>Span ID</label>
                <span class="mono">${span.span_id}</span>
            </div>
            <div class="detail-section">
                <label>Parent ID</label>
                <span class="mono">${span.parent_span_id || '(root)'}</span>
            </div>
            <div class="detail-section">
                <label>Kind</label>
                <span>${span.span_kind || 'INTERNAL'}</span>
            </div>
            <div class="detail-section">
                <label>Duration</label>
                <span>${span.duration_ms}ms</span>
            </div>
            <div class="detail-section">
                <label>Status</label>
                <span class="${span.status_code === 'ERROR' ? 'error' : ''}">${span.status_code}</span>
            </div>
            ${span.status_message ? `
            <div class="detail-section">
                <label>Message</label>
                <span class="error">${this.escapeHtml(span.status_message)}</span>
            </div>
            ` : ''}
        `;
        detailEl.style.display = 'block';
    }

    /**
     * Bind events for trace list view
     */
    bindTraceListEvents() {
        // Search button
        const searchBtn = document.getElementById('trace-search-btn');
        if (searchBtn) {
            searchBtn.addEventListener('click', () => {
                this.searchTraces({
                    service: document.getElementById('trace-service').value,
                    operation: document.getElementById('trace-operation').value,
                    minDuration: parseInt(document.getElementById('trace-min-duration').value) || 0,
                    status: document.getElementById('trace-status').value
                });
            });
        }

        // Trace row click
        this.container.querySelectorAll('.trace-row').forEach(row => {
            row.addEventListener('click', () => {
                const traceId = row.dataset.traceId;
                this.loadTrace(traceId);
            });
        });
    }

    /**
     * Bind events for trace detail view
     */
    bindTraceDetailEvents() {
        // Back button
        const backBtn = document.getElementById('back-to-list');
        if (backBtn) {
            backBtn.addEventListener('click', () => {
                this.renderTraceList();
            });
        }

        // Span row click
        this.container.querySelectorAll('.span-row').forEach(row => {
            row.addEventListener('click', () => {
                // Remove previous selection
                this.container.querySelectorAll('.span-row.selected').forEach(r => {
                    r.classList.remove('selected');
                });
                row.classList.add('selected');
                this.showSpanDetail(row.dataset.spanId);
            });
        });
    }

    /**
     * Show error message
     */
    showError(message) {
        this.container.innerHTML = `<div class="error-message">${this.escapeHtml(message)}</div>`;
    }

    /**
     * Format timestamp for display
     */
    formatTime(timestamp) {
        const date = new Date(timestamp);
        return date.toLocaleTimeString();
    }

    /**
     * Escape HTML to prevent XSS
     */
    escapeHtml(str) {
        if (!str) return '';
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }
}

// CSS styles for trace viewer
const traceViewerStyles = `
.trace-list, .trace-detail {
    padding: 1rem;
}

.trace-search {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 1rem;
    flex-wrap: wrap;
}

.trace-search input, .trace-search select {
    padding: 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-secondary);
    color: var(--text-primary);
}

.trace-search button {
    padding: 0.5rem 1rem;
    background: var(--accent);
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
}

.trace-table {
    width: 100%;
    border-collapse: collapse;
}

.trace-table th, .trace-table td {
    padding: 0.75rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
}

.trace-table th {
    background: var(--bg-secondary);
    font-weight: 600;
}

.trace-row {
    cursor: pointer;
}

.trace-row:hover {
    background: var(--bg-secondary);
}

.trace-row.error {
    background: rgba(239, 68, 68, 0.1);
}

.error-badge {
    background: #ef4444;
    color: white;
    padding: 0.25rem 0.5rem;
    border-radius: 4px;
    font-size: 0.75rem;
}

.trace-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin-bottom: 1rem;
}

.trace-header h3 {
    margin: 0;
    flex: 1;
}

.trace-duration {
    color: var(--text-secondary);
}

.timeline-header {
    margin-bottom: 0.5rem;
    padding-left: 200px;
}

.timeline-labels {
    display: flex;
    justify-content: space-between;
    color: var(--text-secondary);
    font-size: 0.75rem;
}

.span-row {
    display: flex;
    align-items: center;
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--border);
    cursor: pointer;
}

.span-row:hover {
    background: var(--bg-secondary);
}

.span-row.selected {
    background: rgba(59, 130, 246, 0.1);
}

.span-info {
    width: 200px;
    flex-shrink: 0;
    overflow: hidden;
}

.span-service {
    display: block;
    font-weight: 600;
    font-size: 0.85rem;
}

.span-name {
    display: block;
    color: var(--text-secondary);
    font-size: 0.8rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.span-bar-container {
    flex: 1;
    height: 24px;
    position: relative;
    background: var(--bg-secondary);
    border-radius: 4px;
}

.span-bar {
    position: absolute;
    height: 100%;
    background: #3b82f6;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    min-width: 40px;
}

.span-bar.error {
    background: #ef4444;
}

.span-bar.server {
    background: #10b981;
}

.span-bar.client {
    background: #6366f1;
}

.span-duration {
    color: white;
    font-size: 0.75rem;
    padding: 0 0.5rem;
}

.span-detail {
    position: fixed;
    right: 0;
    top: 60px;
    width: 300px;
    height: calc(100vh - 60px);
    background: var(--bg-primary);
    border-left: 1px solid var(--border);
    padding: 1rem;
    display: none;
    overflow-y: auto;
}

.span-detail h4 {
    margin-top: 0;
    margin-bottom: 1rem;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.5rem;
}

.detail-section {
    margin-bottom: 1rem;
}

.detail-section label {
    display: block;
    font-weight: 600;
    font-size: 0.85rem;
    color: var(--text-secondary);
    margin-bottom: 0.25rem;
}

.detail-section .mono {
    font-family: monospace;
    font-size: 0.85rem;
}

.detail-section .error {
    color: #ef4444;
}

.error-message {
    padding: 2rem;
    text-align: center;
    color: #ef4444;
}

#back-to-list {
    padding: 0.5rem 1rem;
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-radius: 4px;
    cursor: pointer;
    color: var(--text-primary);
}

#back-to-list:hover {
    background: var(--bg-tertiary);
}
`;

// Inject styles
if (typeof document !== 'undefined') {
    const styleEl = document.createElement('style');
    styleEl.textContent = traceViewerStyles;
    document.head.appendChild(styleEl);
}

// Export for use
if (typeof window !== 'undefined') {
    window.TraceViewer = TraceViewer;
}
