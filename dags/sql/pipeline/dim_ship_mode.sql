TRUNCATE TABLE analytics.dim_ship_mode RESTART IDENTITY CASCADE;
INSERT INTO analytics.dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode
FROM analytics.sales;