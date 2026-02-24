use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, ObjectName, OrderByExpr,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::data::Value;

/// Parsed query representation
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Table name
    pub table: String,
    /// Selected columns and aggregations
    pub projections: Vec<Projection>,
    /// WHERE conditions
    pub filters: Vec<Filter>,
    /// GROUP BY columns
    pub group_by: Vec<GroupByColumn>,
    /// ORDER BY clauses
    pub order_by: Vec<OrderBy>,
    /// LIMIT
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum Projection {
    /// Simple column reference: SELECT col
    Column(String),
    /// All columns: SELECT *
    Wildcard,
    /// Aggregation: SELECT COUNT(*), SUM(col), etc.
    Aggregation {
        function: AggregateFunction,
        column: Option<String>, // None for COUNT(*)
        alias: Option<String>,
    },
    /// TIME_BUCKET function
    TimeBucket {
        interval_ms: i64,
        column: String,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Percentile(u8), // P50, P90, P99, etc.
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub operator: FilterOperator,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Like,
}

#[derive(Debug, Clone)]
pub enum GroupByColumn {
    Column(String),
    TimeBucket { interval_ms: i64, column: String },
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

/// Parse a SQL query string
pub fn parse_query(sql: &str) -> Result<ParsedQuery, ParseError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    if statements.is_empty() {
        return Err(ParseError::EmptyQuery);
    }

    if statements.len() > 1 {
        return Err(ParseError::MultipleStatements);
    }

    match &statements[0] {
        Statement::Query(query) => parse_select(query),
        _ => Err(ParseError::UnsupportedStatement),
    }
}

fn parse_select(query: &sqlparser::ast::Query) -> Result<ParsedQuery, ParseError> {
    let select = match &*query.body {
        SetExpr::Select(select) => select,
        _ => return Err(ParseError::UnsupportedQuery("Only SELECT queries supported".into())),
    };

    let table = parse_table_name(&select.from)?;
    let projections = parse_projections(&select.projection)?;
    let filters = parse_where(&select.selection)?;
    let group_by = parse_group_by(&select.group_by)?;
    let order_by = parse_order_by(&query.order_by)?;
    let limit = parse_limit(&query.limit)?;

    Ok(ParsedQuery {
        table,
        projections,
        filters,
        group_by,
        order_by,
        limit,
    })
}

fn parse_table_name(from: &[TableWithJoins]) -> Result<String, ParseError> {
    if from.is_empty() {
        return Err(ParseError::MissingTable);
    }

    if from.len() > 1 {
        return Err(ParseError::JoinsNotSupported);
    }

    let table = &from[0];
    if !table.joins.is_empty() {
        return Err(ParseError::JoinsNotSupported);
    }

    match &table.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err(ParseError::UnsupportedTableExpression),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
}

fn parse_projections(items: &[SelectItem]) -> Result<Vec<Projection>, ParseError> {
    let mut projections = Vec::new();

    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                projections.push(parse_projection_expr(expr, None)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                projections.push(parse_projection_expr(expr, Some(alias.value.clone()))?);
            }
            SelectItem::Wildcard(_) => {
                projections.push(Projection::Wildcard);
            }
            _ => return Err(ParseError::UnsupportedProjection),
        }
    }

    Ok(projections)
}

fn parse_projection_expr(expr: &Expr, alias: Option<String>) -> Result<Projection, ParseError> {
    match expr {
        Expr::Identifier(ident) => Ok(Projection::Column(ident.value.clone())),

        Expr::Function(func) => {
            let func_name = func.name.to_string().to_uppercase();

            match func_name.as_str() {
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                    let agg_func = match func_name.as_str() {
                        "COUNT" => AggregateFunction::Count,
                        "SUM" => AggregateFunction::Sum,
                        "AVG" => AggregateFunction::Avg,
                        "MIN" => AggregateFunction::Min,
                        "MAX" => AggregateFunction::Max,
                        _ => unreachable!(),
                    };

                    let column = parse_function_column_arg(&func.args)?;

                    Ok(Projection::Aggregation {
                        function: agg_func,
                        column,
                        alias,
                    })
                }
                "PERCENTILE" | "P50" | "P90" | "P95" | "P99" => {
                    let percentile = match func_name.as_str() {
                        "P50" => 50,
                        "P90" => 90,
                        "P95" => 95,
                        "P99" => 99,
                        "PERCENTILE" => 50,
                        _ => 50,
                    };

                    let column = parse_function_column_arg(&func.args)?;

                    Ok(Projection::Aggregation {
                        function: AggregateFunction::Percentile(percentile),
                        column,
                        alias,
                    })
                }
                "TIME_BUCKET" => {
                    let (interval_ms, column) = parse_time_bucket_args(&func.args)?;
                    Ok(Projection::TimeBucket {
                        interval_ms,
                        column,
                        alias,
                    })
                }
                _ => Err(ParseError::UnsupportedFunction(func_name)),
            }
        }

        Expr::CompoundIdentifier(idents) => {
            let col_name = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
            Ok(Projection::Column(col_name))
        }

        _ => Err(ParseError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

fn parse_function_column_arg(args: &[FunctionArg]) -> Result<Option<String>, ParseError> {
    if args.is_empty() {
        return Ok(None);
    }

    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            Ok(Some(ident.value.clone()))
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(idents))) => {
            Ok(Some(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")))
        }
        FunctionArg::Named { arg: FunctionArgExpr::Expr(Expr::Identifier(ident)), .. } => {
            Ok(Some(ident.value.clone()))
        }
        _ => Err(ParseError::UnsupportedExpression("Complex function argument".into())),
    }
}

fn parse_time_bucket_args(args: &[FunctionArg]) -> Result<(i64, String), ParseError> {
    if args.len() < 2 {
        return Err(ParseError::InvalidTimeBucket);
    }

    // First arg: interval string like '5 minutes'
    let interval_ms = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(SqlValue::SingleQuotedString(s)))) => {
            parse_interval(s)?
        }
        _ => return Err(ParseError::InvalidTimeBucket),
    };

    // Second arg: column name
    let column = match &args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => ident.value.clone(),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(idents))) => {
            idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
        }
        _ => return Err(ParseError::InvalidTimeBucket),
    };

    Ok((interval_ms, column))
}

fn parse_interval(s: &str) -> Result<i64, ParseError> {
    let parts: Vec<&str> = s.trim().split_whitespace().collect();
    if parts.len() != 2 {
        return Err(ParseError::InvalidInterval(s.to_string()));
    }

    let value: i64 = parts[0]
        .parse()
        .map_err(|_| ParseError::InvalidInterval(s.to_string()))?;

    let unit = parts[1].to_lowercase();
    let multiplier = match unit.as_str() {
        "ms" | "millisecond" | "milliseconds" => 1,
        "s" | "second" | "seconds" => 1000,
        "m" | "minute" | "minutes" => 60 * 1000,
        "h" | "hour" | "hours" => 3600 * 1000,
        "d" | "day" | "days" => 86400 * 1000,
        _ => return Err(ParseError::InvalidInterval(s.to_string())),
    };

    Ok(value * multiplier)
}

fn parse_where(selection: &Option<Expr>) -> Result<Vec<Filter>, ParseError> {
    let Some(expr) = selection else {
        return Ok(vec![]);
    };

    parse_filter_expr(expr)
}

fn parse_filter_expr(expr: &Expr) -> Result<Vec<Filter>, ParseError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    let mut filters = parse_filter_expr(left)?;
                    filters.extend(parse_filter_expr(right)?);
                    Ok(filters)
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    let column = extract_column_name(left)?;
                    let value = extract_value(right)?;
                    let operator = match op {
                        BinaryOperator::Eq => FilterOperator::Eq,
                        BinaryOperator::NotEq => FilterOperator::NotEq,
                        BinaryOperator::Lt => FilterOperator::Lt,
                        BinaryOperator::LtEq => FilterOperator::LtEq,
                        BinaryOperator::Gt => FilterOperator::Gt,
                        BinaryOperator::GtEq => FilterOperator::GtEq,
                        _ => unreachable!(),
                    };

                    Ok(vec![Filter {
                        column,
                        operator,
                        value,
                    }])
                }
                _ => Err(ParseError::UnsupportedOperator(format!("{:?}", op))),
            }
        }
        Expr::Like { expr, pattern, .. } => {
            let column = extract_column_name(expr)?;
            let value = extract_value(pattern)?;
            Ok(vec![Filter {
                column,
                operator: FilterOperator::Like,
                value,
            }])
        }
        Expr::Nested(inner) => parse_filter_expr(inner),
        _ => Err(ParseError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

fn extract_column_name(expr: &Expr) -> Result<String, ParseError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => {
            Ok(idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."))
        }
        _ => Err(ParseError::ExpectedColumnName),
    }
}

fn extract_value(expr: &Expr) -> Result<Value, ParseError> {
    match expr {
        Expr::Value(v) => sql_value_to_value(v),
        Expr::UnaryOp { op, expr } => {
            // Handle negative numbers
            if matches!(op, sqlparser::ast::UnaryOperator::Minus) {
                if let Expr::Value(SqlValue::Number(n, _)) = expr.as_ref() {
                    let negated = format!("-{}", n);
                    if let Ok(i) = negated.parse::<i64>() {
                        return Ok(Value::Int64(i));
                    }
                    if let Ok(f) = negated.parse::<f64>() {
                        return Ok(Value::Float64(f));
                    }
                }
            }
            Err(ParseError::ExpectedValue)
        }
        // Handle NOW() - INTERVAL expressions
        Expr::BinaryOp { left, op: BinaryOperator::Minus, right } => {
            if is_now_function(left) {
                if let Expr::Interval(interval) = right.as_ref() {
                    let interval_str = interval.value.to_string();
                    // Remove quotes if present
                    let interval_str = interval_str.trim_matches('\'');
                    let interval_ms = parse_interval(interval_str)?;
                    let now = chrono::Utc::now().timestamp_millis();
                    return Ok(Value::Timestamp(now - interval_ms));
                }
            }
            Err(ParseError::ExpectedValue)
        }
        _ => Err(ParseError::ExpectedValue),
    }
}

fn is_now_function(expr: &Expr) -> bool {
    if let Expr::Function(func) = expr {
        return func.name.to_string().to_uppercase() == "NOW";
    }
    false
}

fn sql_value_to_value(v: &SqlValue) -> Result<Value, ParseError> {
    match v {
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Value::Int64(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Value::Float64(f))
            } else {
                Err(ParseError::InvalidNumber(n.clone()))
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::String(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        _ => Err(ParseError::UnsupportedValue),
    }
}

fn parse_group_by(group_by: &GroupByExpr) -> Result<Vec<GroupByColumn>, ParseError> {
    let exprs = match group_by {
        GroupByExpr::All => return Err(ParseError::UnsupportedExpression("GROUP BY ALL".into())),
        GroupByExpr::Expressions(exprs) => exprs,
    };

    let mut result = Vec::new();

    for expr in exprs {
        match expr {
            Expr::Identifier(ident) => {
                result.push(GroupByColumn::Column(ident.value.clone()));
            }
            Expr::CompoundIdentifier(idents) => {
                let col_name = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                result.push(GroupByColumn::Column(col_name));
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                if func_name == "TIME_BUCKET" {
                    let (interval_ms, column) = parse_time_bucket_args(&func.args)?;
                    result.push(GroupByColumn::TimeBucket { interval_ms, column });
                } else {
                    return Err(ParseError::UnsupportedGroupByExpression);
                }
            }
            _ => return Err(ParseError::UnsupportedGroupByExpression),
        }
    }

    Ok(result)
}

fn parse_order_by(order_by: &[OrderByExpr]) -> Result<Vec<OrderBy>, ParseError> {
    let mut result = Vec::new();

    for expr in order_by {
        let column = match &expr.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(idents) => {
                idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
            }
            _ => return Err(ParseError::UnsupportedOrderByExpression),
        };

        let descending = expr.asc.map(|asc| !asc).unwrap_or(false);

        result.push(OrderBy { column, descending });
    }

    Ok(result)
}

fn parse_limit(limit: &Option<Expr>) -> Result<Option<usize>, ParseError> {
    let Some(expr) = limit else {
        return Ok(None);
    };

    match expr {
        Expr::Value(SqlValue::Number(n, _)) => {
            let limit: usize = n.parse().map_err(|_| ParseError::InvalidLimit)?;
            Ok(Some(limit))
        }
        _ => Err(ParseError::InvalidLimit),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),

    #[error("Empty query")]
    EmptyQuery,

    #[error("Multiple statements not supported")]
    MultipleStatements,

    #[error("Unsupported statement type")]
    UnsupportedStatement,

    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),

    #[error("Missing FROM table")]
    MissingTable,

    #[error("JOINs are not supported")]
    JoinsNotSupported,

    #[error("Unsupported table expression")]
    UnsupportedTableExpression,

    #[error("Unsupported projection")]
    UnsupportedProjection,

    #[error("Unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),

    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),

    #[error("Expected column name")]
    ExpectedColumnName,

    #[error("Expected value")]
    ExpectedValue,

    #[error("Invalid number: {0}")]
    InvalidNumber(String),

    #[error("Unsupported value type")]
    UnsupportedValue,

    #[error("Invalid interval: {0}")]
    InvalidInterval(String),

    #[error("Invalid TIME_BUCKET arguments")]
    InvalidTimeBucket,

    #[error("Unsupported GROUP BY expression")]
    UnsupportedGroupByExpression,

    #[error("Unsupported ORDER BY expression")]
    UnsupportedOrderByExpression,

    #[error("Invalid LIMIT value")]
    InvalidLimit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let query = parse_query("SELECT * FROM events").unwrap();
        assert_eq!(query.table, "events");
        assert_eq!(query.projections.len(), 1);
        assert!(matches!(query.projections[0], Projection::Wildcard));
    }

    #[test]
    fn test_select_columns() {
        let query = parse_query("SELECT event, user_id FROM events").unwrap();
        assert_eq!(query.table, "events");
        assert_eq!(query.projections.len(), 2);
    }

    #[test]
    fn test_select_with_aggregates() {
        let query = parse_query("SELECT COUNT(*), SUM(value), AVG(latency) FROM events").unwrap();
        assert_eq!(query.projections.len(), 3);

        if let Projection::Aggregation { function, column, .. } = &query.projections[0] {
            assert_eq!(*function, AggregateFunction::Count);
            assert!(column.is_none());
        } else {
            panic!("Expected aggregation");
        }
    }

    #[test]
    fn test_where_clause() {
        let query =
            parse_query("SELECT * FROM events WHERE event = 'click' AND value > 100").unwrap();
        assert_eq!(query.filters.len(), 2);
        assert_eq!(query.filters[0].column, "event");
        assert_eq!(query.filters[0].operator, FilterOperator::Eq);
        assert_eq!(query.filters[1].column, "value");
        assert_eq!(query.filters[1].operator, FilterOperator::Gt);
    }

    #[test]
    fn test_group_by() {
        let query = parse_query("SELECT event, COUNT(*) FROM events GROUP BY event").unwrap();
        assert_eq!(query.group_by.len(), 1);
        assert!(matches!(&query.group_by[0], GroupByColumn::Column(c) if c == "event"));
    }

    #[test]
    fn test_order_by_and_limit() {
        let query =
            parse_query("SELECT * FROM events ORDER BY timestamp DESC LIMIT 100").unwrap();
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].column, "timestamp");
        assert!(query.order_by[0].descending);
        assert_eq!(query.limit, Some(100));
    }

    #[test]
    fn test_time_bucket() {
        let query = parse_query(
            "SELECT TIME_BUCKET('5 minutes', timestamp), COUNT(*) FROM events GROUP BY TIME_BUCKET('5 minutes', timestamp)"
        ).unwrap();

        if let Projection::TimeBucket { interval_ms, column, .. } = &query.projections[0] {
            assert_eq!(*interval_ms, 5 * 60 * 1000);
            assert_eq!(column, "timestamp");
        } else {
            panic!("Expected time bucket");
        }
    }

    #[test]
    fn test_interval_parsing() {
        assert_eq!(parse_interval("5 minutes").unwrap(), 5 * 60 * 1000);
        assert_eq!(parse_interval("1 hour").unwrap(), 3600 * 1000);
        assert_eq!(parse_interval("1 day").unwrap(), 86400 * 1000);
        assert_eq!(parse_interval("100 ms").unwrap(), 100);
    }

    #[test]
    fn test_joins_rejected() {
        let result = parse_query("SELECT * FROM a JOIN b ON a.id = b.id");
        assert!(matches!(result, Err(ParseError::JoinsNotSupported)));
    }
}
