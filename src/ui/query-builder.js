// query-builder.js - Build SQL queries from UI state

const QueryBuilder = {
    /**
     * Build an aggregation query from the current state
     */
    buildAggregationQuery(state) {
        const { table, metrics, groupBy, filters, timeRange } = state;

        if (!table) return null;

        // Build SELECT clause
        const selectParts = [];

        // Add group by columns first
        groupBy.forEach(col => {
            selectParts.push(col);
        });

        // Add metrics
        metrics.forEach(m => {
            if (m.col === '*') {
                selectParts.push(`${m.agg}(*)`);
            } else {
                selectParts.push(`${m.agg}(${m.col})`);
            }
        });

        if (selectParts.length === 0) {
            selectParts.push('COUNT(*)');
        }

        // Build WHERE clause
        const whereParts = this.buildWhereClause(filters, timeRange);

        // Build GROUP BY clause
        const groupByClause = groupBy.length > 0 ? `GROUP BY ${groupBy.join(', ')}` : '';

        // Build ORDER BY - order by first group by column or first metric
        let orderByClause = '';
        if (groupBy.length > 0) {
            orderByClause = `ORDER BY ${groupBy[0]}`;
        }

        // Assemble query
        let sql = `SELECT ${selectParts.join(', ')} FROM ${table}`;
        if (whereParts.length > 0) {
            sql += ` WHERE ${whereParts.join(' AND ')}`;
        }
        if (groupByClause) {
            sql += ` ${groupByClause}`;
        }
        if (orderByClause) {
            sql += ` ${orderByClause}`;
        }

        return sql;
    },

    /**
     * Build a sample data query (no aggregation)
     */
    buildSampleQuery(state) {
        const { table, filters, timeRange } = state;

        if (!table) return null;

        const whereParts = this.buildWhereClause(filters, timeRange);

        let sql = `SELECT * FROM ${table}`;
        if (whereParts.length > 0) {
            sql += ` WHERE ${whereParts.join(' AND ')}`;
        }
        sql += ` ORDER BY timestamp DESC LIMIT 100`;

        return sql;
    },

    /**
     * Build WHERE clause parts from filters and time range
     */
    buildWhereClause(filters, timeRange) {
        const parts = [];

        // Add time range filter
        if (timeRange.start !== null && timeRange.end !== null) {
            // Custom time range (from zoom)
            parts.push(`timestamp >= ${timeRange.start}`);
            parts.push(`timestamp <= ${timeRange.end}`);
        } else if (timeRange.preset && timeRange.preset !== 'all') {
            const now = Date.now();
            let start;

            switch (timeRange.preset) {
                case '1h':
                    start = now - (60 * 60 * 1000);
                    break;
                case '6h':
                    start = now - (6 * 60 * 60 * 1000);
                    break;
                case '24h':
                    start = now - (24 * 60 * 60 * 1000);
                    break;
                case '7d':
                    start = now - (7 * 24 * 60 * 60 * 1000);
                    break;
                case '30d':
                    start = now - (30 * 24 * 60 * 60 * 1000);
                    break;
                default:
                    start = null;
            }

            if (start) {
                parts.push(`timestamp >= ${start}`);
            }
        }

        // Add user filters
        filters.forEach(f => {
            if (f.column && f.operator && f.value !== '') {
                const value = this.formatFilterValue(f.value, f.operator);
                parts.push(`${f.column} ${f.operator} ${value}`);
            }
        });

        return parts;
    },

    /**
     * Format filter value based on type
     */
    formatFilterValue(value, operator) {
        // Check if it's a number
        if (!isNaN(value) && value !== '') {
            return value;
        }

        // Check for LIKE operator
        if (operator === 'LIKE') {
            return `'%${value}%'`;
        }

        // String value - quote it
        return `'${value.replace(/'/g, "''")}'`;
    },

    /**
     * Get available operators for a data type
     */
    getOperators(dataType) {
        const numericOps = ['=', '!=', '>', '<', '>=', '<='];
        const stringOps = ['=', '!=', 'LIKE'];
        const boolOps = ['=', '!='];

        switch (dataType?.toLowerCase()) {
            case 'int64':
            case 'float64':
            case 'timestamp':
                return numericOps;
            case 'bool':
                return boolOps;
            case 'string':
            default:
                return stringOps;
        }
    },

    /**
     * Get available aggregations
     */
    getAggregations() {
        return ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];
    }
};

// Export for use in app.js
window.QueryBuilder = QueryBuilder;
