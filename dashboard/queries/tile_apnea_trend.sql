SELECT
    DATE_TRUNC('month', event_date) as month,
    AVG(apnea_count + hypopnea_count) as avg_ahi
FROM ANALYTICS.fct_event_rates
GROUP BY 1
ORDER BY 1
