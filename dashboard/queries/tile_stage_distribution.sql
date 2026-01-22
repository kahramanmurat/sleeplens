SELECT
    age_group,
    AVG(rem_min) as avg_rem,
    AVG(deep_min) as avg_deep,
    AVG(light_min) as avg_light
FROM ANALYTICS.fct_sleep_summary
GROUP BY 1
