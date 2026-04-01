WITH cleaned_data AS (
SELECT
    Location,
    Billing_class,
    Year,

    --  Total Revenue (clean,convert and protect revenur data)
    (
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_January, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_February, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_March, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_April, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_May, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_June, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_July, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_August, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_September, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_October, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_November, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Amount_December, '[^0-9.]', '') AS DOUBLE), 0)
    ) AS total_revenue,

    --  Total Consumption (clean,convert and protect Total Consumption data)
    (
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_January, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_February, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_March, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_April, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_May, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_June, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_July, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_August, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_September, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_October, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_November, '[^0-9.]', '') AS DOUBLE), 0) +
        COALESCE(TRY_CAST(REGEXP_REPLACE(Quantity_December, '[^0-9.]', '') AS DOUBLE), 0)
    ) AS total_quantity

FROM workspace.default.water_billing_2021_to_2026
WHERE Location != '#'  -- Filter out invalid location placeholder
),

aggregated AS (
SELECT
    Location,
    Billing_class,
    Year,
    SUM(total_revenue) AS total_revenue,
    SUM(total_quantity) AS total_quantity
FROM cleaned_data
GROUP BY Location, Billing_class, Year
),

final_metrics AS (
SELECT
    *,

    -- Revenue Efficiency (0 when no consumption)
    CASE
        WHEN total_quantity = 0 THEN 0
        ELSE total_revenue / total_quantity
    END AS revenue_efficiency,

    --  Previous Year Revenue (NULL for first year)
    LAG(total_revenue) OVER (
        PARTITION BY Location, Billing_class
        ORDER BY Year
    ) AS prev_year_revenue,

    -- YoY Change (NULL for first year)
    total_revenue - LAG(total_revenue) OVER (
        PARTITION BY Location, Billing_class
        ORDER BY Year
    ) AS yoy_change,

    -- Rankings
    RANK() OVER (PARTITION BY Year ORDER BY total_revenue DESC) AS top_rank,
    RANK() OVER (PARTITION BY Year ORDER BY total_revenue ASC) AS low_rank

FROM aggregated
)

SELECT
    Location,
    Billing_class,
    Year,
    total_revenue,
    total_quantity,
    revenue_efficiency,
    prev_year_revenue,
    yoy_change,

    CASE
        WHEN top_rank = 1 THEN 'Top Location'
        WHEN low_rank = 1 THEN 'Lowest Location'
        ELSE 'Normal'
    END AS performance_flag

FROM final_metrics
ORDER BY Year DESC, total_revenue DESC