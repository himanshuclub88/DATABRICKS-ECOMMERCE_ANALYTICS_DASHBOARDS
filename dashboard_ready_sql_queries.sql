-- ==========================================================================
-- DASHBOARD-READY SQL QUERIES - JOIN TABLES FOR VISUALIZATION
-- ==========================================================================

CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_dashboards;

-- These queries create 4 consolidated tables optimized for dashboard visualization
-- Each table is designed with specific X-axis, Y-axis, and chart types in mind

-- ==========================================================================
-- TABLE 1: SALES_DASHBOARD_MAIN (Line Charts, Bar Charts)
-- ==========================================================================
-- Purpose: Main dashboard KPIs with time-based trends
-- Chart Types: Line Chart (trends), Bar Chart (comparisons), KPI Cards
-- X-axis: month_year, category, region, channel
-- Y-axis: revenue, orders, customers, avg_order_value

CREATE OR REPLACE TABLE workspace.ecommerce_dashboards.sales_dashboard_main AS
SELECT 
    -- Time dimensions
    mt.year,
    mt.month,
    mt.month_name,
    mt.month_year,

    -- Geographic dimensions
    ra.region,

    -- Product dimensions  
    cp.category,

    -- Channel dimensions
    ch.channel,

    -- Metrics for visualization
    mt.total_revenue as monthly_revenue,
    mt.total_orders as monthly_orders,
    mt.unique_customers as monthly_customers,
    mt.avg_order_value as monthly_aov,

    ra.total_revenue as regional_revenue,
    ra.revenue_percentage as regional_share,

    cp.total_revenue as category_revenue,
    cp.profit_margin_pct,

    ch.total_revenue as channel_revenue,

    -- Calculated metrics for charts
    ROUND((mt.total_revenue / LAG(mt.total_revenue) OVER (ORDER BY mt.year, mt.month) - 1) * 100, 2) as revenue_growth_pct,
    ROUND(mt.total_revenue / mt.total_orders, 2) as calculated_aov,

    -- Dashboard flags
    CASE 
        WHEN mt.total_revenue >= 250000 THEN 'High'
        WHEN mt.total_revenue >= 150000 THEN 'Medium'
        ELSE 'Low'
    END as revenue_tier

FROM workspace.ecommerce_analytics.monthly_trends mt
CROSS JOIN workspace.ecommerce_analytics.regional_analysis ra
CROSS JOIN workspace.ecommerce_analytics.category_performance cp  
CROSS JOIN workspace.ecommerce_analytics.channel_performance ch;

-- ==========================================================================
-- TABLE 2: CUSTOMER_INSIGHTS_DASHBOARD (Pie Charts, Funnel Charts)
-- ==========================================================================
-- Purpose: Customer segmentation and behavior analysis
-- Chart Types: Pie Chart (segments), Funnel Chart (customer journey), Donut Chart
-- X-axis: customer_tier, region, acquisition_month
-- Y-axis: customer_count, total_spent, avg_orders

CREATE OR REPLACE TABLE workspace.ecommerce_dashboards.customer_insights_dashboard AS
SELECT 
    -- Customer segmentation
    ca.customer_tier,

    -- Geographic breakdown
    CASE 
        WHEN ca.customer_id LIKE 'CUST-1%' THEN 'North America'
        WHEN ca.customer_id LIKE 'CUST-2%' THEN 'Europe'  
        WHEN ca.customer_id LIKE 'CUST-3%' THEN 'Asia Pacific'
        ELSE 'Other Regions'
    END as customer_region,

    -- Time-based cohorts
    DATE_TRUNC('month', ca.first_order) as acquisition_month,
    EXTRACT(YEAR FROM ca.first_order) as acquisition_year,

    -- Customer metrics
    COUNT(DISTINCT ca.customer_id) as customer_count,
    SUM(ca.total_spent) as segment_revenue,
    AVG(ca.total_spent) as avg_customer_value,
    AVG(ca.total_orders) as avg_orders_per_customer,
    AVG(ca.order_frequency) as avg_order_frequency,

    -- Percentages for pie charts
    ROUND((COUNT(DISTINCT ca.customer_id) * 100.0 / SUM(COUNT(DISTINCT ca.customer_id)) OVER()), 2) as customer_percentage,
    ROUND((SUM(ca.total_spent) * 100.0 / SUM(SUM(ca.total_spent)) OVER()), 2) as revenue_percentage,

    -- Customer lifecycle metrics
    AVG(ca.customer_lifetime_days) as avg_lifetime_days,

    -- Behavioral flags for filtering
    CASE 
        WHEN AVG(ca.total_orders) >= 5 THEN 'Loyal'
        WHEN AVG(ca.total_orders) >= 2 THEN 'Regular' 
        ELSE 'One-time'
    END as loyalty_segment,

    CASE
        WHEN AVG(ca.order_frequency) >= 2 THEN 'High Frequency'
        WHEN AVG(ca.order_frequency) >= 1 THEN 'Medium Frequency'
        ELSE 'Low Frequency'  
    END as frequency_segment

FROM workspace.ecommerce_analytics.customer_analysis ca
GROUP BY 
    ca.customer_tier,
    CASE 
        WHEN ca.customer_id LIKE 'CUST-1%' THEN 'North America'
        WHEN ca.customer_id LIKE 'CUST-2%' THEN 'Europe'
        WHEN ca.customer_id LIKE 'CUST-3%' THEN 'Asia Pacific'
        ELSE 'Other Regions'
    END,
    DATE_TRUNC('month', ca.first_order),
    EXTRACT(YEAR FROM ca.first_order);

-- ==========================================================================  
-- TABLE 3: PRODUCT_PERFORMANCE_DASHBOARD (Bar Charts, Heatmap)
-- ==========================================================================
-- Purpose: Product and category performance analysis
-- Chart Types: Horizontal Bar Chart, Heatmap, Treemap
-- X-axis: product_name, category, revenue_rank
-- Y-axis: total_revenue, total_sold, unique_customers

CREATE OR REPLACE TABLE workspace.ecommerce_dashboards.product_performance_dashboard AS
SELECT 
    -- Product dimensions
    pp.product_name,
    pp.category,
    pp.revenue_rank,

    -- Category performance
    cp.total_orders as category_total_orders,
    cp.profit_margin_pct as category_profit_margin,

    -- Product metrics
    pp.total_revenue as product_revenue,
    pp.total_sold as units_sold,
    pp.unique_customers as product_customers,
    pp.avg_price,

    -- Performance indicators
    ROUND((pp.total_revenue * 100.0 / cp.total_revenue), 2) as product_category_share,

    -- Ranking within category
    ROW_NUMBER() OVER (PARTITION BY pp.category ORDER BY pp.total_revenue DESC) as category_rank,

    -- Performance tiers for color coding
    CASE 
        WHEN pp.revenue_rank <= 5 THEN 'Top Performer'
        WHEN pp.revenue_rank <= 10 THEN 'Good Performer'
        WHEN pp.revenue_rank <= 15 THEN 'Average Performer'
        ELSE 'Low Performer'
    END as performance_tier,

    -- Price positioning
    CASE
        WHEN pp.avg_price >= 500 THEN 'Premium'
        WHEN pp.avg_price >= 100 THEN 'Mid-range'
        ELSE 'Budget'
    END as price_segment,

    -- Volume vs Value analysis
    CASE
        WHEN pp.total_sold >= 100 AND pp.total_revenue >= 50000 THEN 'High Volume High Value'
        WHEN pp.total_sold >= 100 AND pp.total_revenue < 50000 THEN 'High Volume Low Value'
        WHEN pp.total_sold < 100 AND pp.total_revenue >= 50000 THEN 'Low Volume High Value'
        ELSE 'Low Volume Low Value'
    END as volume_value_segment

FROM workspace.ecommerce_analytics.product_performance pp
JOIN workspace.ecommerce_analytics.category_performance cp 
    ON pp.category = cp.category;

-- ==========================================================================
-- TABLE 4: EXECUTIVE_SUMMARY_DASHBOARD (KPI Cards, Gauge Charts)
-- ==========================================================================
-- Purpose: High-level executive metrics and trends
-- Chart Types: KPI Cards, Gauge Charts, Sparklines, Comparison Charts
-- X-axis: time_period, comparison_type
-- Y-axis: kpi_value, target_value, variance

CREATE OR REPLACE TABLE workspace.ecommerce_dashboards.executive_summary_dashboard AS
WITH current_metrics AS (
    SELECT 
        -- Current period metrics
        SUM(ds.total_revenue) as current_revenue,
        SUM(ds.total_orders) as current_orders,
        SUM(ds.unique_customers) as current_customers,
        AVG(ds.avg_order_value) as current_aov,
        COUNT(DISTINCT ds.order_date) as active_days
    FROM workspace.ecommerce_analytics.daily_sales_summary ds
    WHERE ds.order_date >= DATE_SUB(CURRENT_DATE(), 30)
),
previous_metrics AS (
    SELECT 
        -- Previous period metrics (30 days before)
        SUM(ds.total_revenue) as previous_revenue,
        SUM(ds.total_orders) as previous_orders,
        SUM(ds.unique_customers) as previous_customers,
        AVG(ds.avg_order_value) as previous_aov
    FROM workspace.ecommerce_analytics.daily_sales_summary ds
    WHERE ds.order_date >= DATE_SUB(CURRENT_DATE(), 60) 
      AND ds.order_date < DATE_SUB(CURRENT_DATE(), 30)
),
targets AS (
    SELECT 
        -- Business targets (can be adjusted)
        3000000 as revenue_target,
        15000 as orders_target, 
        5000 as customers_target,
        200 as aov_target
)
SELECT 
    -- KPI Names and Values
    'Total Revenue' as kpi_name,
    cm.current_revenue as current_value,
    pm.previous_revenue as previous_value,
    t.revenue_target as target_value,

    -- Performance calculations
    ROUND(((cm.current_revenue - pm.previous_revenue) / pm.previous_revenue) * 100, 2) as period_growth_pct,
    ROUND((cm.current_revenue / t.revenue_target) * 100, 2) as target_achievement_pct,

    -- Status indicators for gauge charts
    CASE 
        WHEN cm.current_revenue >= t.revenue_target THEN 'Exceeded'
        WHEN cm.current_revenue >= t.revenue_target * 0.9 THEN 'On Track'
        WHEN cm.current_revenue >= t.revenue_target * 0.7 THEN 'Behind'
        ELSE 'Critical'
    END as performance_status,

    -- Trend direction for sparklines
    CASE
        WHEN cm.current_revenue > pm.previous_revenue THEN 'UP'
        WHEN cm.current_revenue < pm.previous_revenue THEN 'DOWN' 
        ELSE 'FLAT'
    END as trend_direction,

    -- Additional context
    CURRENT_DATE() as report_date,
    'Last 30 Days' as time_period

FROM current_metrics cm
CROSS JOIN previous_metrics pm  
CROSS JOIN targets t

UNION ALL

-- Orders KPI
SELECT 
    'Total Orders' as kpi_name,
    cm.current_orders as current_value,
    pm.previous_orders as previous_value, 
    t.orders_target as target_value,

    ROUND(((cm.current_orders - pm.previous_orders) / pm.previous_orders) * 100, 2) as period_growth_pct,
    ROUND((cm.current_orders / t.orders_target) * 100, 2) as target_achievement_pct,

    CASE 
        WHEN cm.current_orders >= t.orders_target THEN 'Exceeded'
        WHEN cm.current_orders >= t.orders_target * 0.9 THEN 'On Track'
        WHEN cm.current_orders >= t.orders_target * 0.7 THEN 'Behind'
        ELSE 'Critical'
    END as performance_status,

    CASE
        WHEN cm.current_orders > pm.previous_orders THEN 'UP'
        WHEN cm.current_orders < pm.previous_orders THEN 'DOWN'
        ELSE 'FLAT'
    END as trend_direction,

    CURRENT_DATE() as report_date,
    'Last 30 Days' as time_period

FROM current_metrics cm
CROSS JOIN previous_metrics pm
CROSS JOIN targets t

UNION ALL

-- Customers KPI  
SELECT 
    'Total Customers' as kpi_name,
    cm.current_customers as current_value,
    pm.previous_customers as previous_value,
    t.customers_target as target_value,

    ROUND(((cm.current_customers - pm.previous_customers) / pm.previous_customers) * 100, 2) as period_growth_pct,
    ROUND((cm.current_customers / t.customers_target) * 100, 2) as target_achievement_pct,

    CASE 
        WHEN cm.current_customers >= t.customers_target THEN 'Exceeded'
        WHEN cm.current_customers >= t.customers_target * 0.9 THEN 'On Track'
        WHEN cm.current_customers >= t.customers_target * 0.7 THEN 'Behind'
        ELSE 'Critical'
    END as performance_status,

    CASE
        WHEN cm.current_customers > pm.previous_customers THEN 'UP'
        WHEN cm.current_customers < pm.previous_customers THEN 'DOWN'
        ELSE 'FLAT'
    END as trend_direction,

    CURRENT_DATE() as report_date,
    'Last 30 Days' as time_period

FROM current_metrics cm
CROSS JOIN previous_metrics pm
CROSS JOIN targets t

UNION ALL

-- Average Order Value KPI
SELECT 
    'Average Order Value' as kpi_name,
    cm.current_aov as current_value,
    pm.previous_aov as previous_value,
    t.aov_target as target_value,

    ROUND(((cm.current_aov - pm.previous_aov) / pm.previous_aov) * 100, 2) as period_growth_pct,
    ROUND((cm.current_aov / t.aov_target) * 100, 2) as target_achievement_pct,

    CASE 
        WHEN cm.current_aov >= t.aov_target THEN 'Exceeded'
        WHEN cm.current_aov >= t.aov_target * 0.9 THEN 'On Track'
        WHEN cm.current_aov >= t.aov_target * 0.7 THEN 'Behind'
        ELSE 'Critical'
    END as performance_status,

    CASE
        WHEN cm.current_aov > pm.previous_aov THEN 'UP'
        WHEN cm.current_aov < pm.previous_aov THEN 'DOWN'
        ELSE 'FLAT'
    END as trend_direction,

    CURRENT_DATE() as report_date,
    'Last 30 Days' as time_period

FROM current_metrics cm
CROSS JOIN previous_metrics pm
CROSS JOIN targets t;