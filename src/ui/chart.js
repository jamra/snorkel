// chart.js - ECharts wrapper

const ChartManager = {
    chart: null,
    chartType: 'bar',
    onBrushSelection: null,

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

        if (this.chartType === 'table') {
            this.showTableView();
            return;
        }

        // Identify dimensions (strings) and metrics (numbers)
        const dimensions = [];
        const metrics = [];

        columns.forEach((col, idx) => {
            const val = rows[0][idx];
            if (typeof val === 'number') {
                metrics.push({ name: col, idx });
            } else {
                dimensions.push({ name: col, idx });
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

        // X-axis labels from first dimension, or row indices
        const xLabels = dimensions.length > 0
            ? rows.map(r => String(r[dimensions[0].idx]))
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

        const option = {
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

        console.log('Setting chart option:', option);

        try {
            this.chart.setOption(option, true);
            console.log('Chart updated successfully');
        } catch (e) {
            console.error('Failed to set chart option:', e);
        }
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

    clearBrush() {
        if (this.chart) {
            this.chart.dispatchAction({ type: 'brush', areas: [] });
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
