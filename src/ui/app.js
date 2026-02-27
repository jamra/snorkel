// app.js - Main application logic

const App = {
    // Application state
    state: {
        table: '',
        schema: [],
        metrics: [],
        groupBy: [],
        filters: [],
        timeRange: {
            start: null,
            end: null,
            preset: 'all'
        },
        zoomStack: [],
        viewMode: 'visual',
        chartType: 'bar',
        lastQuery: null,
        lastResults: null
    },

    elements: {},

    async init() {
        this.cacheElements();
        this.bindEvents();
        this.initChart();
        await this.loadTables();
        this.updateStatus('Ready');
    },

    cacheElements() {
        this.elements = {
            btnVisual: document.getElementById('btn-visual'),
            btnQuery: document.getElementById('btn-query'),
            btnForms: document.getElementById('btn-forms'),
            btnTraces: document.getElementById('btn-traces'),
            sidebarVisual: document.getElementById('sidebar-visual'),
            sidebarQuery: document.getElementById('sidebar-query'),
            sidebarForms: document.getElementById('sidebar-forms'),
            visualContent: document.getElementById('visual-content'),
            queryContent: document.getElementById('query-content'),
            tracesContent: document.getElementById('traces-content'),
            tableSelect: document.getElementById('table-select'),
            metricsList: document.getElementById('metrics-list'),
            groupByList: document.getElementById('groupby-list'),
            filtersList: document.getElementById('filters-list'),
            btnAddFilter: document.getElementById('btn-add-filter'),
            timeRange: document.getElementById('time-range'),
            btnRun: document.getElementById('btn-run'),
            chartContainer: document.getElementById('chart'),
            chartTypeToggle: document.querySelector('.chart-type-toggle'),
            zoomInfo: document.getElementById('zoom-info'),
            btnResetZoom: document.getElementById('btn-reset-zoom'),
            samplesHead: document.getElementById('samples-head'),
            samplesBody: document.getElementById('samples-body'),
            sqlInput: document.getElementById('sql-input'),
            btnExecute: document.getElementById('btn-execute'),
            resultsHead: document.getElementById('results-head'),
            resultsBody: document.getElementById('results-body'),
            tablesList: document.getElementById('tables-list'),
            exampleQueries: document.getElementById('example-queries'),
            statusMessage: document.getElementById('status-message'),
            statusStats: document.getElementById('status-stats'),
            thresholdValue: document.getElementById('threshold-value'),
            thresholdType: document.getElementById('threshold-type'),
            btnApplyThreshold: document.getElementById('btn-apply-threshold'),
            btnClearThreshold: document.getElementById('btn-clear-threshold')
        };
    },

    bindEvents() {
        this.elements.btnVisual.addEventListener('click', () => this.setViewMode('visual'));
        this.elements.btnQuery.addEventListener('click', () => this.setViewMode('query'));
        this.elements.btnForms.addEventListener('click', () => this.setViewMode('forms'));
        this.elements.btnTraces.addEventListener('click', () => this.setViewMode('traces'));
        this.elements.tableSelect.addEventListener('change', (e) => this.onTableChange(e.target.value));
        this.elements.btnAddFilter.addEventListener('click', () => this.addFilter());
        this.elements.timeRange.addEventListener('change', (e) => {
            this.state.timeRange = { start: null, end: null, preset: e.target.value };
            this.state.zoomStack = [];
            this.updateZoomInfo();
            this.runQuery(); // Auto-run when time range changes
        });
        this.elements.btnRun.addEventListener('click', () => this.runQuery());
        this.elements.chartTypeToggle.addEventListener('click', (e) => {
            if (e.target.tagName === 'BUTTON') {
                this.setChartType(e.target.dataset.type);
            }
        });
        this.elements.btnResetZoom.addEventListener('click', () => this.resetZoom());
        this.elements.btnExecute.addEventListener('click', () => this.executeRawQuery());
        this.elements.sqlInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
                this.executeRawQuery();
            }
        });
        this.elements.exampleQueries.addEventListener('click', (e) => {
            const item = e.target.closest('.checkbox-item');
            if (item && item.dataset.query) {
                this.elements.sqlInput.value = item.dataset.query;
            }
        });

        // Threshold controls
        this.elements.btnApplyThreshold.addEventListener('click', () => this.applyThreshold());
        this.elements.btnClearThreshold.addEventListener('click', () => this.clearThreshold());
        this.elements.thresholdValue.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.applyThreshold();
        });

        // Set default chart type
        this.setChartType('bar');
    },

    applyThreshold() {
        const value = this.elements.thresholdValue.value;
        const type = this.elements.thresholdType.value;
        if (value !== '') {
            ChartManager.setThreshold(value, type);
            if (this.state.lastResults) {
                ChartManager.update(this.state.lastResults.rows, this.state.lastResults.columns);
            }
            this.updateStatus(`Threshold set: ${type} ${value}`);
        }
    },

    clearThreshold() {
        ChartManager.clearThreshold();
        this.elements.thresholdValue.value = '';
        if (this.state.lastResults) {
            ChartManager.update(this.state.lastResults.rows, this.state.lastResults.columns);
        }
        this.updateStatus('Threshold cleared');
    },

    initChart() {
        console.log('App.initChart called, container element:', this.elements.chartContainer);
        ChartManager.init(this.elements.chartContainer, (start, end) => {
            this.onBrushSelection(start, end);
        });
    },

    async loadTables() {
        try {
            const response = await fetch('/tables');
            const data = await response.json();

            this.elements.tableSelect.innerHTML = '';

            if (data.tables.length === 0) {
                this.elements.tableSelect.innerHTML = '<option value="">No tables found</option>';
                return;
            }

            data.tables.forEach(t => {
                const option = document.createElement('option');
                option.value = t.name;
                option.textContent = `${t.name} (${t.row_count} rows)`;
                this.elements.tableSelect.appendChild(option);
            });

            // Populate query mode tables
            this.elements.tablesList.innerHTML = '';
            data.tables.forEach(t => {
                const div = document.createElement('div');
                div.className = 'checkbox-item';
                div.innerHTML = `<label>${t.name} (${t.row_count} rows)</label>`;
                div.addEventListener('click', () => {
                    this.elements.sqlInput.value = `SELECT * FROM ${t.name} LIMIT 100`;
                });
                this.elements.tablesList.appendChild(div);
            });

            if (data.tables.length > 0) {
                await this.onTableChange(data.tables[0].name);
            }
        } catch (error) {
            this.updateStatus('Failed to load tables: ' + error.message, true);
        }
    },

    async onTableChange(tableName) {
        this.state.table = tableName;
        this.state.metrics = [];
        this.state.groupBy = [];
        this.state.filters = [];

        if (!tableName) {
            this.elements.metricsList.innerHTML = '';
            this.elements.groupByList.innerHTML = '';
            return;
        }

        try {
            const response = await fetch(`/tables/${tableName}/schema`);
            if (!response.ok) {
                throw new Error('Failed to load schema');
            }
            const data = await response.json();
            // Schema returns 'type' not 'data_type'
            this.state.schema = (data.columns || []).map(c => ({
                name: c.name,
                type: c.type || c.data_type || 'STRING'
            }));
            this.renderMetrics();
            this.renderGroupBy();
            this.renderFilters();

            // Auto-run a simple query
            this.runQuery();
        } catch (error) {
            this.updateStatus('Failed to load schema: ' + error.message, true);
            console.error('Schema error:', error);
        }
    },

    renderMetrics() {
        const container = this.elements.metricsList;
        container.innerHTML = '';
        this.state.metrics = [];

        // Add COUNT(*) as default
        const countRow = this.createMetricRow('*', 'COUNT', true);
        container.appendChild(countRow);
        this.state.metrics.push({ col: '*', agg: 'COUNT' });

        // Add numeric columns
        this.state.schema.forEach(col => {
            if (col.type === 'INT64' || col.type === 'FLOAT64') {
                const row = this.createMetricRow(col.name, 'AVG', false);
                container.appendChild(row);
            }
        });
    },

    createMetricRow(colName, defaultAgg, checked) {
        const row = document.createElement('div');
        row.className = 'metric-row';

        const id = `metric-${colName.replace(/[^a-z0-9]/gi, '')}`;
        const displayName = colName === '*' ? 'COUNT(*)' : colName;

        row.innerHTML = `
            <input type="checkbox" id="${id}" ${checked ? 'checked' : ''}>
            <select class="metric-agg">
                <option value="COUNT" ${defaultAgg === 'COUNT' ? 'selected' : ''}>COUNT</option>
                <option value="SUM" ${defaultAgg === 'SUM' ? 'selected' : ''}>SUM</option>
                <option value="AVG" ${defaultAgg === 'AVG' ? 'selected' : ''}>AVG</option>
                <option value="MIN" ${defaultAgg === 'MIN' ? 'selected' : ''}>MIN</option>
                <option value="MAX" ${defaultAgg === 'MAX' ? 'selected' : ''}>MAX</option>
            </select>
            <label for="${id}">${colName === '*' ? '*' : colName}</label>
        `;

        const checkbox = row.querySelector('input');
        const aggSelect = row.querySelector('select');

        // Hide agg selector for COUNT(*)
        if (colName === '*') {
            aggSelect.style.display = 'none';
        }

        checkbox.addEventListener('change', (e) => {
            if (e.target.checked) {
                this.state.metrics.push({ col: colName, agg: aggSelect.value });
            } else {
                this.state.metrics = this.state.metrics.filter(m => m.col !== colName);
            }
        });

        aggSelect.addEventListener('change', (e) => {
            const metric = this.state.metrics.find(m => m.col === colName);
            if (metric) {
                metric.agg = e.target.value;
            }
        });

        return row;
    },

    renderGroupBy() {
        const container = this.elements.groupByList;
        container.innerHTML = '';

        // Get string/categorical columns for grouping
        const groupableCols = this.state.schema.filter(col =>
            col.type === 'STRING' || col.name === 'timestamp'
        );

        if (groupableCols.length === 0) {
            container.innerHTML = '<div class="empty-message">No groupable columns</div>';
            return;
        }

        groupableCols.forEach(col => {
            if (col.name === 'timestamp') return; // Skip timestamp for now

            const row = document.createElement('div');
            row.className = 'groupby-row';

            const id = `groupby-${col.name}`;
            row.innerHTML = `
                <input type="checkbox" id="${id}">
                <label for="${id}">${col.name}</label>
            `;

            row.querySelector('input').addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.state.groupBy.push(col.name);
                } else {
                    this.state.groupBy = this.state.groupBy.filter(c => c !== col.name);
                }
            });

            container.appendChild(row);
        });
    },

    renderFilters() {
        const container = this.elements.filtersList;
        container.innerHTML = '';

        this.state.filters.forEach((filter, index) => {
            const row = this.createFilterRow(filter, index);
            container.appendChild(row);
        });
    },

    createFilterRow(filter, index) {
        const row = document.createElement('div');
        row.className = 'filter-row';

        // Column dropdown
        const colSelect = document.createElement('select');
        colSelect.className = 'filter-col';
        this.state.schema.forEach(col => {
            const option = document.createElement('option');
            option.value = col.name;
            option.textContent = col.name;
            option.dataset.type = col.type;
            if (col.name === filter.column) option.selected = true;
            colSelect.appendChild(option);
        });

        // Operator dropdown
        const opSelect = document.createElement('select');
        opSelect.className = 'filter-op';
        const operators = this.getOperatorsForType(filter.columnType || 'STRING');
        operators.forEach(op => {
            const option = document.createElement('option');
            option.value = op.value;
            option.textContent = op.label;
            if (op.value === filter.operator) option.selected = true;
            opSelect.appendChild(option);
        });

        // Value input
        const valueInput = document.createElement('input');
        valueInput.type = 'text';
        valueInput.className = 'filter-value';
        valueInput.value = filter.value || '';
        valueInput.placeholder = 'value';

        // Remove button
        const removeBtn = document.createElement('button');
        removeBtn.className = 'btn-icon';
        removeBtn.innerHTML = '×';
        removeBtn.title = 'Remove filter';

        // Events
        colSelect.addEventListener('change', (e) => {
            this.state.filters[index].column = e.target.value;
            const selectedOption = e.target.options[e.target.selectedIndex];
            this.state.filters[index].columnType = selectedOption.dataset.type;
            // Re-render operators
            this.renderFilters();
        });

        opSelect.addEventListener('change', (e) => {
            this.state.filters[index].operator = e.target.value;
        });

        valueInput.addEventListener('input', (e) => {
            this.state.filters[index].value = e.target.value;
        });

        removeBtn.addEventListener('click', () => {
            this.state.filters.splice(index, 1);
            this.renderFilters();
        });

        row.appendChild(colSelect);
        row.appendChild(opSelect);
        row.appendChild(valueInput);
        row.appendChild(removeBtn);

        return row;
    },

    getOperatorsForType(type) {
        const stringOps = [
            { value: '=', label: 'equals' },
            { value: '!=', label: 'not equals' },
            { value: 'LIKE', label: 'contains' }
        ];
        const numericOps = [
            { value: '=', label: '=' },
            { value: '!=', label: '!=' },
            { value: '>', label: '>' },
            { value: '<', label: '<' },
            { value: '>=', label: '>=' },
            { value: '<=', label: '<=' }
        ];

        switch (type) {
            case 'INT64':
            case 'FLOAT64':
            case 'TIMESTAMP':
                return numericOps;
            default:
                return stringOps;
        }
    },

    addFilter() {
        const defaultCol = this.state.schema[0];
        this.state.filters.push({
            column: defaultCol?.name || '',
            columnType: defaultCol?.type || 'STRING',
            operator: '=',
            value: ''
        });
        this.renderFilters();
    },

    setViewMode(mode) {
        this.state.viewMode = mode;
        this.elements.btnVisual.classList.toggle('active', mode === 'visual');
        this.elements.btnQuery.classList.toggle('active', mode === 'query');
        this.elements.btnForms.classList.toggle('active', mode === 'forms');
        this.elements.btnTraces.classList.toggle('active', mode === 'traces');
        this.elements.sidebarVisual.classList.toggle('active', mode === 'visual');
        this.elements.sidebarQuery.classList.toggle('active', mode === 'query');
        this.elements.sidebarForms.classList.toggle('active', mode === 'forms');
        this.elements.visualContent.classList.toggle('active', mode === 'visual');
        this.elements.queryContent.classList.toggle('active', mode === 'query' || mode === 'forms');
        this.elements.tracesContent.classList.toggle('active', mode === 'traces');

        if (mode === 'visual') {
            ChartManager.resize();
        }

        if (mode === 'traces') {
            this.initTraceViewer();
        }
    },

    initTraceViewer() {
        if (!this.traceViewer) {
            this.traceViewer = new TraceViewer(this.elements.tracesContent);
        }
        // Load initial trace list
        this.traceViewer.searchTraces();
    },

    setChartType(type) {
        const prevType = this.state.chartType;
        this.state.chartType = type;
        ChartManager.setType(type);

        this.elements.chartTypeToggle.querySelectorAll('button').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.type === type);
        });

        // Re-run query if switching between bar and line/area (query changes for time series)
        const wasTimeSeries = prevType === 'line' || prevType === 'area';
        const isTimeSeries = type === 'line' || type === 'area';
        if (wasTimeSeries !== isTimeSeries && this.state.groupBy.length > 0) {
            this.runQuery();
        } else if (this.state.lastResults) {
            ChartManager.update(this.state.lastResults.rows, this.state.lastResults.columns);
        }
    },

    async runQuery() {
        const sql = QueryBuilder.buildAggregationQuery(this.state, this.state.chartType);
        const sampleSql = QueryBuilder.buildSampleQuery(this.state);

        if (!sql) {
            this.updateStatus('Please select a table', true);
            return;
        }

        this.state.lastQuery = sql;
        this.elements.btnRun.disabled = true;
        this.updateStatus('Running query...');

        console.log('Running query:', sql);

        try {
            const [chartResult, sampleResult] = await Promise.all([
                this.executeQuery(sql),
                this.executeQuery(sampleSql)
            ]);

            console.log('Chart result:', chartResult);

            this.state.lastResults = chartResult;
            ChartManager.update(chartResult.rows, chartResult.columns);
            this.updateSamplesTable(sampleResult);

            this.updateStatus('Query completed');
            this.elements.statusStats.textContent =
                `${chartResult.row_count} rows | ${chartResult.rows_scanned} scanned | ${chartResult.execution_time_ms}ms`;

        } catch (error) {
            this.updateStatus('Query failed: ' + error.message, true);
            console.error('Query error:', error);
        } finally {
            this.elements.btnRun.disabled = false;
        }
    },

    async executeRawQuery() {
        const sql = this.elements.sqlInput.value.trim();
        if (!sql) {
            this.updateStatus('Please enter a query', true);
            return;
        }

        this.elements.btnExecute.disabled = true;
        this.updateStatus('Running query...');

        try {
            const result = await this.executeQuery(sql);
            this.updateResultsTable(result);
            this.updateStatus('Query completed');
            this.elements.statusStats.textContent =
                `${result.row_count} rows | ${result.rows_scanned} scanned | ${result.execution_time_ms}ms`;
        } catch (error) {
            this.updateStatus('Query failed: ' + error.message, true);
        } finally {
            this.elements.btnExecute.disabled = false;
        }
    },

    async executeQuery(sql) {
        const response = await fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Query failed');
        }

        return response.json();
    },

    updateSamplesTable(result) {
        this.elements.samplesHead.innerHTML = '<tr>' +
            result.columns.map(c => `<th>${c}</th>`).join('') + '</tr>';

        this.elements.samplesBody.innerHTML = result.rows.map(row =>
            '<tr>' + row.map(cell => `<td>${this.formatCell(cell)}</td>`).join('') + '</tr>'
        ).join('');
    },

    updateResultsTable(result) {
        this.elements.resultsHead.innerHTML = '<tr>' +
            result.columns.map(c => `<th>${c}</th>`).join('') + '</tr>';

        this.elements.resultsBody.innerHTML = result.rows.map(row =>
            '<tr>' + row.map(cell => `<td>${this.formatCell(cell)}</td>`).join('') + '</tr>'
        ).join('');
    },

    formatCell(value) {
        if (value === null) return '<span style="color:#666">null</span>';
        if (typeof value === 'number') {
            if (value > 1000000000000) {
                return new Date(value).toLocaleString();
            }
            if (!Number.isInteger(value)) {
                return value.toFixed(2);
            }
        }
        return String(value);
    },

    onBrushSelection(startIndex, endIndex) {
        if (!this.state.lastResults || !this.state.lastResults.rows) return;

        const rows = this.state.lastResults.rows;
        if (rows.length < 2) {
            this.updateStatus('Need more data points to zoom');
            return;
        }

        const tsColIndex = this.state.lastResults.columns.findIndex(c =>
            c === 'timestamp' || c.includes('time') || c.includes('bucket')
        );

        if (tsColIndex === -1) {
            this.updateStatus('Brush selection requires a time column');
            return;
        }

        const startRow = Math.max(0, Math.floor(startIndex));
        const endRow = Math.min(rows.length - 1, Math.ceil(endIndex));

        // Don't zoom if selection is too small or covers everything
        if (endRow <= startRow || (startRow === 0 && endRow === rows.length - 1)) {
            return;
        }

        const startTs = rows[startRow][tsColIndex];
        const endTs = rows[endRow][tsColIndex];

        // Don't zoom if timestamps are the same or invalid
        if (startTs === endTs || startTs === undefined || endTs === undefined) {
            return;
        }

        this.state.zoomStack.push({ ...this.state.timeRange });
        this.state.timeRange = {
            start: Math.min(startTs, endTs),
            end: Math.max(startTs, endTs),
            preset: null
        };

        this.updateZoomInfo();
        this.runQuery();
    },

    resetZoom() {
        // Fully reset to the dropdown preset, clearing all zoom state
        const preset = this.elements.timeRange.value || 'all';
        this.state.timeRange = { start: null, end: null, preset: preset };
        this.state.zoomStack = [];

        this.updateZoomInfo();
        ChartManager.clearBrush();
        this.runQuery();
    },

    updateZoomInfo() {
        const { start, end, preset } = this.state.timeRange;

        if (start !== null && end !== null) {
            const startStr = new Date(start).toLocaleString();
            const endStr = new Date(end).toLocaleString();
            this.elements.zoomInfo.textContent = `${startStr} - ${endStr}`;
            this.elements.btnResetZoom.style.display = 'inline-block';
        } else if (preset) {
            this.elements.zoomInfo.textContent = `Time range: ${preset}`;
            this.elements.btnResetZoom.style.display = this.state.zoomStack.length > 0 ? 'inline-block' : 'none';
        } else {
            this.elements.zoomInfo.textContent = '';
            this.elements.btnResetZoom.style.display = 'none';
        }

        if (preset) {
            this.elements.timeRange.value = preset;
        }
    },

    updateStatus(message, isError = false) {
        this.elements.statusMessage.textContent = message;
        this.elements.statusMessage.className = isError ? 'error' : '';
        if (!isError) {
            this.elements.statusStats.textContent = '';
        }
    }
};

document.addEventListener('DOMContentLoaded', () => App.init());
