# Role-Based Access Control (RBAC) Model

> Defines data access roles, permissions, and row/column-level security
> for the Pharmacy Operations Analytics Platform.

---

## Role Definitions

### analyst_commercial
- **Access**: `marts.commercial.*`, `marts.executive.*`
- **Row filter**: None
- **Column filter**: Exclude PII columns (NPI, prescriber names)
- **Use case**: Commercial team analyzing drug spending trends, market share, and therapeutic area performance.

### analyst_customer_success
- **Access**: `marts.customer_success.*`, `marts.executive.*`
- **Row filter**: None
- **Column filter**: Full access (includes NPI for prescriber engagement tracking)
- **Use case**: Customer Success team tracking prescriber engagement, retention, and specialty adoption.

### analyst_pharmacy_ops
- **Access**: `marts.pharmacy_ops.*`, `marts.executive.*`
- **Row filter**: None
- **Column filter**: Full access
- **Use case**: Pharmacy Operations team monitoring fill volumes, costs, brand/generic ratios.

### partner_manufacturer
- **Access**: `marts.enterprise.*`
- **Row filter**: `WHERE therapeutic_area IN (partner_assigned_areas)`
- **Column filter**: Exclude competitor-specific columns. Aggregated only.
- **Use case**: External pharmaceutical manufacturer partners viewing their drug performance and market share. Each partner sees only their contracted therapeutic areas.

### admin_data
- **Access**: All schemas (`raw`, `staging`, `intermediate`, `marts`, `snapshots`)
- **Row filter**: None
- **Column filter**: None
- **Use case**: Data & Analytics team for development, debugging, and governance.

### viewer_executive
- **Access**: `marts.executive.*`, `marts.data_quality.*`
- **Row filter**: None
- **Column filter**: Aggregated metrics only, no row-level detail
- **Use case**: C-suite and board-level reporting.

---

## Implementation Notes

### Snowflake
```sql
-- Create roles
CREATE ROLE analyst_commercial;
CREATE ROLE analyst_customer_success;
CREATE ROLE analyst_pharmacy_ops;
CREATE ROLE partner_manufacturer;
CREATE ROLE admin_data;
CREATE ROLE viewer_executive;

-- Grant schema access
GRANT USAGE ON SCHEMA marts TO ROLE analyst_commercial;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO ROLE analyst_commercial;

-- Row-level security for partner_manufacturer
CREATE OR REPLACE ROW ACCESS POLICY partner_ta_filter AS
  (therapeutic_area VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() != 'PARTNER_MANUFACTURER'
    OR therapeutic_area IN (
      SELECT ta FROM partner_assignments
      WHERE partner_id = CURRENT_USER()
    );
```

### Power BI (Row-Level Security)
```dax
// RLS filter for partner_manufacturer role
[therapeutic_area] IN
  SELECTCOLUMNS(
    FILTER(PartnerAssignments, [partner_id] = USERPRINCIPALNAME()),
    "ta", [therapeutic_area]
  )
```

### BigQuery (Row-Level Security)
```sql
-- Create row access policy
CREATE ROW ACCESS POLICY partner_filter
  ON `pharma_ops.mart_manufacturer_performance_report`
  GRANT TO ("group:partner_manufacturer@company.com")
  FILTER USING (
    therapeutic_area IN (
      SELECT ta FROM `pharma_ops.partner_assignments`
      WHERE partner_email = SESSION_USER()
    )
  );
```

### Databricks (Unity Catalog Row Filters)
```sql
-- Create row filter function
CREATE FUNCTION pharma_ops.partner_row_filter(therapeutic_area STRING)
  RETURN IF(
    IS_ACCOUNT_GROUP_MEMBER('partner_manufacturer'),
    therapeutic_area IN (
      SELECT ta FROM pharma_ops.partner_assignments
      WHERE partner_id = CURRENT_USER()
    ),
    TRUE  -- non-partner roles see all rows
  );

-- Apply to table
ALTER TABLE pharma_ops.mart_manufacturer_performance_report
  SET ROW FILTER pharma_ops.partner_row_filter ON (therapeutic_area);
```
