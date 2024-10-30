Proposed Star Schema Structure
Fact Tables

Fact_Withdrawals:
Metrics: transaction amount, transaction count.
Dimensions: timestamp, user, interface, currency, transaction status.
Fact_Events:
Metrics: event count.
Dimensions: timestamp, user, event type.
Dimension Tables

Dim_User: Contains user attributes such as user_id, jurisdiction, level, and other demographic or registration details.
Dim_Time: Holds time-based attributes, making it easier to aggregate data by day, week, month, etc.
Dim_Interface: Represents the interface used for the transaction (e.g., app, web).
Dim_Currency: Contains information about currencies used in the transactions (e.g., MXN, USD).
Dim_Event_Type: Represents different types of events (e.g., login, level_change_up).