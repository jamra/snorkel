// query-forms.js - Pre-built query forms for common analytics patterns

const QueryForms = {
    // Pre-defined form templates
    templates: [
        {
            id: 'error-rate',
            name: 'Error Rate',
            description: 'Track error rates over time',
            icon: '⚠️',
            fields: [
                { name: 'table', type: 'table', label: 'Table', required: true },
                { name: 'error_column', type: 'column', label: 'Error Column', required: true },
                { name: 'error_value', type: 'text', label: 'Error Value', default: 'error', placeholder: 'e.g., error, 500' },
                { name: 'time_bucket', type: 'select', label: 'Time Bucket', options: ['1m', '5m', '15m', '1h', '1d'], default: '5m' },
                { name: 'time_range', type: 'select', label: 'Time Range', options: ['1h', '6h', '24h', '7d'], default: '1h' }
            ],
            buildQuery: (values) => {
                const bucket = QueryForms.parseBucket(values.time_bucket);
                const range = QueryForms.parseRange(values.time_range);
                return `SELECT
    TIME_BUCKET(timestamp, ${bucket}) as bucket,
    COUNT(*) as total,
    SUM(CASE WHEN ${values.error_column} = '${values.error_value}' THEN 1 ELSE 0 END) as errors
FROM ${values.table}
WHERE timestamp >= ${range.start}
GROUP BY bucket
ORDER BY bucket`;
            }
        },
        {
            id: 'top-n',
            name: 'Top N',
            description: 'Find the most frequent values',
            icon: '🏆',
            fields: [
                { name: 'table', type: 'table', label: 'Table', required: true },
                { name: 'group_column', type: 'column', label: 'Group By', required: true },
                { name: 'limit', type: 'number', label: 'Limit', default: 10, min: 1, max: 100 },
                { name: 'time_range', type: 'select', label: 'Time Range', options: ['1h', '6h', '24h', '7d', 'all'], default: '24h' }
            ],
            buildQuery: (values) => {
                const range = QueryForms.parseRange(values.time_range);
                let whereClause = '';
                if (values.time_range !== 'all') {
                    whereClause = `WHERE timestamp >= ${range.start}`;
                }
                return `SELECT ${values.group_column}, COUNT(*) as count
FROM ${values.table}
${whereClause}
GROUP BY ${values.group_column}
ORDER BY count DESC
LIMIT ${values.limit}`;
            }
        },
        {
            id: 'latency-percentiles',
            name: 'Latency Percentiles',
            description: 'Calculate p50, p95, p99 latencies',
            icon: '⏱️',
            fields: [
                { name: 'table', type: 'table', label: 'Table', required: true },
                { name: 'latency_column', type: 'column', label: 'Latency Column', required: true, filter: 'numeric' },
                { name: 'group_column', type: 'column', label: 'Group By (optional)', required: false },
                { name: 'time_range', type: 'select', label: 'Time Range', options: ['1h', '6h', '24h', '7d'], default: '1h' }
            ],
            buildQuery: (values) => {
                const range = QueryForms.parseRange(values.time_range);
                const groupBy = values.group_column ? `${values.group_column}, ` : '';
                const groupByClause = values.group_column ? `GROUP BY ${values.group_column}` : '';
                return `SELECT
    ${groupBy}PERCENTILE(${values.latency_column}, 0.50) as p50,
    PERCENTILE(${values.latency_column}, 0.95) as p95,
    PERCENTILE(${values.latency_column}, 0.99) as p99,
    AVG(${values.latency_column}) as avg,
    COUNT(*) as count
FROM ${values.table}
WHERE timestamp >= ${range.start}
${groupByClause}`;
            }
        },
        {
            id: 'time-series',
            name: 'Time Series',
            description: 'Aggregate metrics over time',
            icon: '📈',
            fields: [
                { name: 'table', type: 'table', label: 'Table', required: true },
                { name: 'metric_column', type: 'column', label: 'Metric Column', required: true, filter: 'numeric' },
                { name: 'aggregation', type: 'select', label: 'Aggregation', options: ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'], default: 'AVG' },
                { name: 'time_bucket', type: 'select', label: 'Time Bucket', options: ['1m', '5m', '15m', '1h', '1d'], default: '5m' },
                { name: 'time_range', type: 'select', label: 'Time Range', options: ['1h', '6h', '24h', '7d'], default: '1h' }
            ],
            buildQuery: (values) => {
                const bucket = QueryForms.parseBucket(values.time_bucket);
                const range = QueryForms.parseRange(values.time_range);
                return `SELECT
    TIME_BUCKET(timestamp, ${bucket}) as bucket,
    ${values.aggregation}(${values.metric_column}) as value
FROM ${values.table}
WHERE timestamp >= ${range.start}
GROUP BY bucket
ORDER BY bucket`;
            }
        },
        {
            id: 'compare-groups',
            name: 'Compare Groups',
            description: 'Compare metrics across categories',
            icon: '📊',
            fields: [
                { name: 'table', type: 'table', label: 'Table', required: true },
                { name: 'group_column', type: 'column', label: 'Group By', required: true },
                { name: 'metric_column', type: 'column', label: 'Metric Column', required: true, filter: 'numeric' },
                { name: 'aggregation', type: 'select', label: 'Aggregation', options: ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'], default: 'AVG' },
                { name: 'time_range', type: 'select', label: 'Time Range', options: ['1h', '6h', '24h', '7d', 'all'], default: '24h' }
            ],
            buildQuery: (values) => {
                const range = QueryForms.parseRange(values.time_range);
                let whereClause = '';
                if (values.time_range !== 'all') {
                    whereClause = `WHERE timestamp >= ${range.start}`;
                }
                return `SELECT
    ${values.group_column},
    ${values.aggregation}(${values.metric_column}) as value,
    COUNT(*) as count
FROM ${values.table}
${whereClause}
GROUP BY ${values.group_column}
ORDER BY value DESC`;
            }
        },
        {
            id: 'anomaly-detection',
            name: 'Anomaly Detection',
            description: 'Find outliers based on deviation',
            icon: '🔍',
            fields: [
                { name: 'table', type: 'table', label: 'Table', required: true },
                { name: 'metric_column', type: 'column', label: 'Metric Column', required: true, filter: 'numeric' },
                { name: 'threshold', type: 'number', label: 'Threshold (std devs)', default: 2, min: 1, max: 5 },
                { name: 'time_range', type: 'select', label: 'Time Range', options: ['1h', '6h', '24h', '7d'], default: '24h' }
            ],
            buildQuery: (values) => {
                const range = QueryForms.parseRange(values.time_range);
                return `SELECT
    timestamp,
    ${values.metric_column}
FROM ${values.table}
WHERE timestamp >= ${range.start}
  AND ${values.metric_column} > (
    SELECT AVG(${values.metric_column}) + ${values.threshold} *
           (MAX(${values.metric_column}) - MIN(${values.metric_column})) / 4
    FROM ${values.table}
    WHERE timestamp >= ${range.start}
  )
ORDER BY ${values.metric_column} DESC
LIMIT 100`;
            }
        }
    ],

    // Cached schema data
    tables: [],
    currentSchema: {},

    // Parse time bucket to milliseconds
    parseBucket(bucket) {
        const units = { 'm': 60000, 'h': 3600000, 'd': 86400000 };
        const match = bucket.match(/(\d+)([mhd])/);
        if (match) {
            return parseInt(match[1]) * units[match[2]];
        }
        return 300000; // default 5 minutes
    },

    // Parse time range to start timestamp
    parseRange(range) {
        const now = Date.now();
        const units = { 'h': 3600000, 'd': 86400000 };
        const match = range.match(/(\d+)([hd])/);
        if (match) {
            const start = now - (parseInt(match[1]) * units[match[2]]);
            return { start, end: now };
        }
        return { start: 0, end: now };
    },

    // Initialize the forms panel
    async init() {
        await this.loadTables();
        this.render();
        this.bindEvents();
    },

    // Load available tables
    async loadTables() {
        try {
            const response = await fetch('/tables');
            const data = await response.json();
            this.tables = data.tables || [];
        } catch (e) {
            console.error('Failed to load tables:', e);
            this.tables = [];
        }
    },

    // Load schema for a table
    async loadSchema(tableName) {
        if (this.currentSchema[tableName]) {
            return this.currentSchema[tableName];
        }
        try {
            const response = await fetch(`/tables/${tableName}/schema`);
            const data = await response.json();
            this.currentSchema[tableName] = (data.columns || []).map(c => ({
                name: c.name,
                type: c.type || c.data_type || 'STRING'
            }));
            return this.currentSchema[tableName];
        } catch (e) {
            console.error('Failed to load schema:', e);
            return [];
        }
    },

    // Render the forms panel
    render() {
        const container = document.getElementById('forms-panel');
        if (!container) return;

        container.innerHTML = `
            <div class="forms-header">
                <h3>Query Templates</h3>
                <p class="forms-description">Pre-built queries for common analytics patterns</p>
            </div>
            <div class="forms-list">
                ${this.templates.map(t => `
                    <div class="form-card" data-form-id="${t.id}">
                        <div class="form-card-icon">${t.icon}</div>
                        <div class="form-card-content">
                            <div class="form-card-name">${t.name}</div>
                            <div class="form-card-desc">${t.description}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
            <div class="form-builder" id="form-builder" style="display: none;">
                <div class="form-builder-header">
                    <button class="btn-back" id="btn-form-back">← Back</button>
                    <span class="form-builder-title" id="form-builder-title"></span>
                </div>
                <div class="form-fields" id="form-fields"></div>
                <div class="form-preview" id="form-preview"></div>
                <div class="form-actions">
                    <button class="btn-secondary" id="btn-copy-query">Copy SQL</button>
                    <button class="btn-primary" id="btn-run-form">Run Query</button>
                </div>
            </div>
        `;
    },

    // Bind event handlers
    bindEvents() {
        const container = document.getElementById('forms-panel');
        if (!container) return;

        // Form card clicks
        container.addEventListener('click', async (e) => {
            const card = e.target.closest('.form-card');
            if (card) {
                const formId = card.dataset.formId;
                await this.openForm(formId);
            }

            if (e.target.id === 'btn-form-back') {
                this.closeForm();
            }

            if (e.target.id === 'btn-run-form') {
                this.runForm();
            }

            if (e.target.id === 'btn-copy-query') {
                this.copyQuery();
            }
        });

        // Field changes
        container.addEventListener('change', (e) => {
            if (e.target.closest('.form-field')) {
                this.updatePreview();
            }
        });

        container.addEventListener('input', (e) => {
            if (e.target.closest('.form-field')) {
                this.updatePreview();
            }
        });
    },

    // Open a form builder
    async openForm(formId) {
        const template = this.templates.find(t => t.id === formId);
        if (!template) return;

        this.currentTemplate = template;

        const formsList = document.querySelector('.forms-list');
        const formBuilder = document.getElementById('form-builder');
        const title = document.getElementById('form-builder-title');
        const fields = document.getElementById('form-fields');

        formsList.style.display = 'none';
        formBuilder.style.display = 'block';
        title.textContent = `${template.icon} ${template.name}`;

        // Render fields
        fields.innerHTML = '';
        for (const field of template.fields) {
            const fieldHtml = await this.renderField(field);
            fields.innerHTML += fieldHtml;
        }

        // Initial preview
        this.updatePreview();
    },

    // Close form builder
    closeForm() {
        const formsList = document.querySelector('.forms-list');
        const formBuilder = document.getElementById('form-builder');

        formsList.style.display = 'block';
        formBuilder.style.display = 'none';
        this.currentTemplate = null;
    },

    // Render a form field
    async renderField(field) {
        let input = '';

        switch (field.type) {
            case 'table':
                input = `<select class="form-input" name="${field.name}" ${field.required ? 'required' : ''}>
                    <option value="">Select table...</option>
                    ${this.tables.map(t => `<option value="${t.name}">${t.name}</option>`).join('')}
                </select>`;
                break;

            case 'column':
                input = `<select class="form-input" name="${field.name}" ${field.required ? 'required' : ''}
                    data-filter="${field.filter || ''}">
                    <option value="">Select column...</option>
                </select>`;
                break;

            case 'select':
                input = `<select class="form-input" name="${field.name}">
                    ${field.options.map(o => `<option value="${o}" ${o === field.default ? 'selected' : ''}>${o}</option>`).join('')}
                </select>`;
                break;

            case 'number':
                input = `<input type="number" class="form-input" name="${field.name}"
                    value="${field.default || ''}"
                    min="${field.min || ''}"
                    max="${field.max || ''}"
                    ${field.required ? 'required' : ''}>`;
                break;

            case 'text':
            default:
                input = `<input type="text" class="form-input" name="${field.name}"
                    value="${field.default || ''}"
                    placeholder="${field.placeholder || ''}"
                    ${field.required ? 'required' : ''}>`;
                break;
        }

        return `
            <div class="form-field" data-field-name="${field.name}" data-field-type="${field.type}">
                <label class="form-label">${field.label}${field.required ? ' *' : ''}</label>
                ${input}
            </div>
        `;
    },

    // Update column dropdowns when table changes
    async updateColumnSelects(tableName) {
        const schema = await this.loadSchema(tableName);
        const columnSelects = document.querySelectorAll('.form-field[data-field-type="column"] select');

        columnSelects.forEach(select => {
            const filter = select.dataset.filter;
            const currentValue = select.value;

            select.innerHTML = '<option value="">Select column...</option>';

            schema.forEach(col => {
                if (filter === 'numeric' && col.type !== 'INT64' && col.type !== 'FLOAT64') {
                    return;
                }
                const option = document.createElement('option');
                option.value = col.name;
                option.textContent = col.name;
                select.appendChild(option);
            });

            // Restore value if still valid
            if (currentValue && Array.from(select.options).some(o => o.value === currentValue)) {
                select.value = currentValue;
            }
        });
    },

    // Get current form values
    getFormValues() {
        const values = {};
        const fields = document.querySelectorAll('.form-field');

        fields.forEach(field => {
            const name = field.dataset.fieldName;
            const input = field.querySelector('input, select');
            if (input) {
                values[name] = input.value;
            }
        });

        return values;
    },

    // Update query preview
    async updatePreview() {
        const preview = document.getElementById('form-preview');
        if (!this.currentTemplate || !preview) return;

        const values = this.getFormValues();

        // Update column selects when table changes
        if (values.table) {
            await this.updateColumnSelects(values.table);
        }

        try {
            const sql = this.currentTemplate.buildQuery(values);
            preview.innerHTML = `<pre class="sql-preview">${this.highlightSql(sql)}</pre>`;
            this.currentQuery = sql;
        } catch (e) {
            preview.innerHTML = `<div class="preview-error">Fill in all required fields</div>`;
            this.currentQuery = null;
        }
    },

    // Simple SQL syntax highlighting
    highlightSql(sql) {
        const keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'AND', 'OR', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DESC', 'ASC'];
        const functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'PERCENTILE', 'TIME_BUCKET'];

        let highlighted = sql
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');

        keywords.forEach(kw => {
            highlighted = highlighted.replace(new RegExp(`\\b${kw}\\b`, 'gi'), `<span class="sql-keyword">${kw}</span>`);
        });

        functions.forEach(fn => {
            highlighted = highlighted.replace(new RegExp(`\\b${fn}\\b`, 'gi'), `<span class="sql-function">${fn}</span>`);
        });

        // Numbers
        highlighted = highlighted.replace(/\b(\d+)\b/g, '<span class="sql-number">$1</span>');

        // Strings
        highlighted = highlighted.replace(/'([^']*)'/g, '<span class="sql-string">\'$1\'</span>');

        return highlighted;
    },

    // Run the current form query
    async runForm() {
        if (!this.currentQuery) {
            alert('Please fill in all required fields');
            return;
        }

        try {
            const response = await fetch('/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql: this.currentQuery })
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.error || 'Query failed');
            }

            const result = await response.json();

            // Switch to visual mode and show results
            if (typeof App !== 'undefined') {
                App.setViewMode('query');
                App.elements.sqlInput.value = this.currentQuery;
                App.updateResultsTable(result);
                App.updateStatus('Query completed');
                App.elements.statusStats.textContent =
                    `${result.row_count} rows | ${result.rows_scanned} scanned | ${result.execution_time_ms}ms`;
            }
        } catch (e) {
            alert('Query failed: ' + e.message);
        }
    },

    // Copy query to clipboard
    async copyQuery() {
        if (!this.currentQuery) return;

        try {
            await navigator.clipboard.writeText(this.currentQuery);
            const btn = document.getElementById('btn-copy-query');
            const originalText = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => btn.textContent = originalText, 2000);
        } catch (e) {
            console.error('Failed to copy:', e);
        }
    }
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    // Delay init to ensure tables are loaded
    setTimeout(() => QueryForms.init(), 500);
});
