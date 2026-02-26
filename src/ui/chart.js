// chart.js - ECharts wrapper

const ChartManager = {
    chart: null,
    chartType: 'bar',
    onBrushSelection: null,
    lastDataLength: 0,
    zoomDebounceTimer: null,

    init(container, onBrushSelection) {
        console.log('ChartManager.init called, container:', container);

        if (!container) {
            console.error('Chart container not found!');
            return;
        }

        // Check if echarts is available
        if (typeof echarts === 'undefined') {
            console.error('ECharts library not loaded!');
            return;
        }

        try {
            this.chart = echarts.init(container);
            console.log('ECharts initialized successfully');
        } catch (e) {
            console.error('Failed to initialize ECharts:', e);
            return;
        }

        this.onBrushSelection = onBrushSelection;

        // Listen for dataZoom events with debounce to apply zoom after user stops dragging
        this.chart.on('dataZoom', (params) => {
            if (!this.lastDataLength || !this.onBrushSelection) return;

            const option = this.chart.getOption();
            if (!option.dataZoom || !option.dataZoom[0]) return;

            const { start, end } = option.dataZoom[0];

            // Store the current zoom selection
            this.zoomStart = Math.floor((start / 100) * this.lastDataLength);
            this.zoomEnd = Math.ceil((end / 100) * this.lastDataLength) - 1;
            this.isZoomed = (start > 0 || end < 100);

            // Debounce: apply zoom 500ms after user stops dragging
            if (this.zoomDebounceTimer) {
                clearTimeout(this.zoomDebounceTimer);
            }

            if (this.isZoomed) {
                this.zoomDebounceTimer = setTimeout(() => {
                    this.onBrushSelection(this.zoomStart, this.zoomEnd);
                }, 500);
            }
        });

        window.addEventListener('resize', () => {
            if (this.chart) this.chart.resize();
        });

        this.showEmpty();
    },

    showEmpty() {
        if (!this.chart) return;

        this.chart.setOption({
            title: {
                text: 'Select metrics and click Run Query',
                left: 'center',
                top: 'center',
                textStyle: {
                    color: '#666',
                    fontSize: 14,
                    fontWeight: 'normal'
                }
            },
            xAxis: { show: false },
            yAxis: { show: false },
            series: []
        }, true);
    },

    setType(type) {
        this.chartType = type;
    },

    update(rows, columns) {
        console.log('ChartManager.update:', { rows, columns, chartType: this.chartType });

        if (!this.chart) {
            console.error('Chart not initialized!');
            return;
        }

        if (!rows || rows.length === 0) {
            this.showEmpty();
            return;
        }

        this.lastDataLength = rows.length;

        if (this.chartType === 'table') {
            this.showTableView();
            return;
        }

        // Identify dimensions (strings/timestamps) and metrics (numbers)
        const dimensions = [];
        const metrics = [];

        columns.forEach((col, idx) => {
            const val = rows[0][idx];
            const colLower = col.toLowerCase();
            // Treat timestamp/time_bucket columns as dimensions, not metrics
            if (colLower === 'timestamp' || colLower.includes('time_bucket') || colLower.includes('time') || colLower.includes('date')) {
                dimensions.push({ name: col, idx, isTimestamp: true });
            } else if (typeof val === 'number') {
                metrics.push({ name: col, idx });
            } else {
                dimensions.push({ name: col, idx, isTimestamp: false });
            }
        });

        console.log('Parsed - dimensions:', dimensions, 'metrics:', metrics);

        if (metrics.length === 0) {
            this.chart.setOption({
                title: {
                    text: 'No numeric columns to display',
                    left: 'center',
                    top: 'center',
                    textStyle: { color: '#888', fontSize: 14 }
                },
                xAxis: { show: false },
                yAxis: { show: false },
                series: []
            }, true);
            return;
        }

        let option;

        // For line/area charts with 2+ dimensions, pivot: first dim = X-axis, second dim = series
        if ((this.chartType === 'line' || this.chartType === 'area') && dimensions.length >= 2) {
            option = this.buildPivotedLineChart(rows, dimensions, metrics);
        } else {
            option = this.buildStandardChart(rows, dimensions, metrics);
        }

        console.log('Setting chart option:', option);

        try {
            this.chart.setOption(option, true);
            console.log('Chart updated successfully');
        } catch (e) {
            console.error('Failed to set chart option:', e);
        }
    },

    // Format timestamp for display
    formatTimestamp(ts) {
        const date = new Date(ts);
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    },

    // Standard chart: combine all dimensions for X-axis labels
    buildStandardChart(rows, dimensions, metrics) {
        // Combine ALL dimension columns for X-axis labels (e.g., "US | click")
        const xLabels = dimensions.length > 0
            ? rows.map(r => dimensions.map(d => {
                const val = r[d.idx];
                return d.isTimestamp ? this.formatTimestamp(val) : String(val);
            }).join(' | '))
            : rows.map((_, i) => `#${i+1}`);

        // Create series for each metric
        const series = metrics.map((m, i) => ({
            name: m.name,
            type: this.chartType === 'area' ? 'line' : this.chartType,
            areaStyle: this.chartType === 'area' ? { opacity: 0.4 } : undefined,
            data: rows.map(r => r[m.idx]),
            itemStyle: {
                borderRadius: this.chartType === 'bar' ? [3, 3, 0, 0] : 0
            }
        }));

        return {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'axis',
                backgroundColor: 'rgba(30,30,50,0.9)',
                borderColor: '#444',
                textStyle: { color: '#fff' }
            },
            legend: {
                show: metrics.length > 1,
                top: 5,
                textStyle: { color: '#aaa' }
            },
            grid: {
                left: 50,
                right: 20,
                top: metrics.length > 1 ? 40 : 20,
                bottom: 40
            },
            xAxis: {
                type: 'category',
                data: xLabels,
                axisLine: { lineStyle: { color: '#444' } },
                axisLabel: {
                    color: '#aaa',
                    rotate: xLabels.length > 6 ? 30 : 0,
                    fontSize: 11
                }
            },
            yAxis: {
                type: 'value',
                axisLine: { show: false },
                axisLabel: { color: '#aaa', fontSize: 11 },
                splitLine: { lineStyle: { color: '#333', type: 'dashed' } }
            },
            series: series,
            color: ['#e94560', '#4ade80', '#60a5fa', '#fbbf24', '#a78bfa']
        };
    },

    // Pivoted line chart: first dimension = X-axis, remaining dimensions = separate lines
    buildPivotedLineChart(rows, dimensions, metrics) {
        const xDim = dimensions[0];
        const seriesDims = dimensions.slice(1); // All dimensions after the first become series
        const metric = metrics[0]; // Use first metric for Y values

        // Helper to build series key from all non-X dimensions
        const getSeriesKey = (row) => seriesDims.map(d => String(row[d.idx])).join(' | ');

        // Get unique X values and series values
        const xValuesSet = new Set();
        const seriesValuesSet = new Set();
        rows.forEach(r => {
            xValuesSet.add(r[xDim.idx]); // Keep raw value for sorting
            seriesValuesSet.add(getSeriesKey(r));
        });

        // Sort X values (important for timestamps)
        let xValues = Array.from(xValuesSet);
        if (xDim.isTimestamp) {
            xValues.sort((a, b) => a - b);
        }
        const xLabels = xValues.map(v => xDim.isTimestamp ? this.formatTimestamp(v) : String(v));
        const seriesValues = Array.from(seriesValuesSet).sort();

        // Build lookup: { xValue: { seriesKey: metricValue } }
        const dataMap = {};
        rows.forEach(r => {
            const x = r[xDim.idx]; // Use raw value as key
            const s = getSeriesKey(r);
            const v = r[metric.idx];
            if (!dataMap[x]) dataMap[x] = {};
            dataMap[x][s] = v;
        });

        // Create a series for each unique value in the second dimension
        const series = seriesValues.map((sv, i) => ({
            name: sv,
            type: 'line',
            areaStyle: this.chartType === 'area' ? { opacity: 0.4 } : undefined,
            data: xValues.map(x => dataMap[x]?.[sv] ?? null),
            smooth: true,
            connectNulls: true
        }));

        return {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'axis',
                backgroundColor: 'rgba(30,30,50,0.9)',
                borderColor: '#444',
                textStyle: { color: '#fff' }
            },
            legend: {
                show: true,
                top: 5,
                textStyle: { color: '#aaa' }
            },
            toolbox: {
                show: true,
                right: 20,
                feature: {
                    dataZoom: {
                        yAxisIndex: 'none',
                        title: { zoom: 'Zoom', back: 'Reset' }
                    }
                },
                iconStyle: { borderColor: '#aaa' }
            },
            dataZoom: [
                {
                    type: 'inside',
                    xAxisIndex: 0,
                    filterMode: 'none'
                },
                {
                    type: 'slider',
                    xAxisIndex: 0,
                    height: 20,
                    bottom: 5,
                    borderColor: '#444',
                    fillerColor: 'rgba(233,69,96,0.2)',
                    handleStyle: { color: '#e94560' },
                    textStyle: { color: '#aaa' },
                    filterMode: 'none'
                }
            ],
            grid: {
                left: 50,
                right: 20,
                top: 40,
                bottom: 60
            },
            xAxis: {
                type: 'category',
                data: xLabels,
                axisLine: { lineStyle: { color: '#444' } },
                axisLabel: {
                    color: '#aaa',
                    rotate: xLabels.length > 6 ? 30 : 0,
                    fontSize: 11
                }
            },
            yAxis: {
                type: 'value',
                name: metric.name,
                nameTextStyle: { color: '#aaa' },
                axisLine: { show: false },
                axisLabel: { color: '#aaa', fontSize: 11 },
                splitLine: { lineStyle: { color: '#333', type: 'dashed' } }
            },
            series: series,
            color: ['#e94560', '#4ade80', '#60a5fa', '#fbbf24', '#a78bfa', '#f472b6', '#34d399', '#818cf8']
        };
    },

    showTableView() {
        if (!this.chart) return;

        this.chart.setOption({
            title: {
                text: 'Table view - see Sample Data below',
                left: 'center',
                top: 'center',
                textStyle: { color: '#666', fontSize: 14 }
            },
            xAxis: { show: false },
            yAxis: { show: false },
            series: []
        }, true);
    },

    // Check if chart is currently zoomed (not showing full range)
    hasZoomSelection() {
        return this.isZoomed === true;
    },

    // Get the current zoom selection as row indices
    getZoomSelection() {
        if (!this.isZoomed) return null;
        return { startIdx: this.zoomStart, endIdx: this.zoomEnd };
    },

    // Apply the zoom selection - calls the onBrushSelection callback
    applyZoomSelection() {
        if (this.isZoomed && this.onBrushSelection) {
            this.onBrushSelection(this.zoomStart, this.zoomEnd);
        }
    },

    clearBrush() {
        // Cancel any pending zoom application
        if (this.zoomDebounceTimer) {
            clearTimeout(this.zoomDebounceTimer);
            this.zoomDebounceTimer = null;
        }

        this.isZoomed = false;
        this.zoomStart = 0;
        this.zoomEnd = 0;

        if (this.chart) {
            this.chart.dispatchAction({ type: 'brush', areas: [] });
            // Reset dataZoom to full range
            this.chart.dispatchAction({
                type: 'dataZoom',
                start: 0,
                end: 100
            });
        }
    },

    resize() {
        if (this.chart) {
            this.chart.resize();
        }
    }
};

window.ChartManager = ChartManager;
console.log('chart.js loaded');
