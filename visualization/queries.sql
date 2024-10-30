-- active_users_per_day.sql
SELECT event_timestamp::date AS active_date, 
       COUNT(DISTINCT user_id) AS active_users
FROM fact_events
GROUP BY active_date
ORDER BY active_date;

-- users_without_deposit.sql
SELECT u.user_id
FROM dim_user u
LEFT JOIN fact_deposits d ON u.user_id = d.user_id
WHERE d.user_id IS NULL;

-- users_with_more_than_5_deposits.sql
SELECT user_id
FROM fact_deposits
GROUP BY user_id
HAVING COUNT(id) > 5;

-- last_login_per_user.sql
SELECT user_id, 
       MAX(event_timestamp) AS last_login
FROM fact_events
WHERE event_name = 'login'
GROUP BY user_id
ORDER BY last_login DESC;

-- logins_between_dates.sql
SELECT user_id, 
       COUNT(id) AS login_count
FROM fact_events
WHERE event_name = 'login'
  AND event_timestamp BETWEEN '2020-01-01' AND '2024-12-31'
GROUP BY user_id
ORDER BY login_count DESC;

-- unique_currencies_deposited_per_day.sql
SELECT event_timestamp::date AS deposit_date, 
       COUNT(DISTINCT currency) AS unique_currencies
FROM fact_deposits
GROUP BY deposit_date
ORDER BY deposit_date;

-- unique_currencies_withdrew_per_day.sql
SELECT event_timestamp::date AS withdrawal_date, 
       COUNT(DISTINCT currency) AS unique_currencies
FROM fact_withdrawals
GROUP BY withdrawal_date
ORDER BY withdrawal_date;

-- total_amount_deposited_per_currency_per_day.sql
SELECT event_timestamp::date AS deposit_date, 
       currency, 
       SUM(amount) AS total_amount
FROM fact_deposits
GROUP BY deposit_date, currency
ORDER BY deposit_date, currency;
