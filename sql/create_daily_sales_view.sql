CREATE OR REPLACE VIEW daily_sales AS
SELECT
    o.order_date::date AS order_day,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE o.status = 'completed') AS completed_orders,
    COUNT(*) FILTER (WHERE o.status = 'cancelled') AS cancelled_orders,
    COALESCE(
        SUM(o.revenue) FILTER (WHERE o.status = 'completed'),
        0
    ) AS daily_revenue,
    COALESCE(
        AVG(o.revenue) FILTER (WHERE o.status = 'completed'),
        0
    ) AS average_order_value
FROM orders o
GROUP BY o.order_date::date;
