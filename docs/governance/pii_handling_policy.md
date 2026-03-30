# PII and Data Privacy Handling Policy

> Data classification, handling procedures, and compliance requirements
> for the Pharmacy Operations Analytics Platform.

---

## Data Classification

### This Project (Portfolio)
All data in this project comes from CMS public use files (PUFs).
These files are de-identified per 45 CFR 164.514 (HIPAA Safe Harbor).

**What this means:**
- No Protected Health Information (PHI) is present
- No patient names, SSNs, dates of birth, or addresses
- NPI (National Provider Identifier) is a public identifier, not PHI
- Prescriber names associated with NPIs are publicly available via NPPES

### CMS Small-Cell Suppression
CMS suppresses data for any cell with fewer than 11 beneficiaries to
prevent potential re-identification through small-cell analysis. This
project enforces this rule in all models via the `min_beneficiary_threshold`
dbt variable (default: 11).

```sql
-- Applied in every mart model
WHERE total_beneficiaries >= {{ var('min_beneficiary_threshold') }}
```

---

## Production Adaptation (Real Pharmacy Data)

If this architecture were adapted for production use with real patient
data (e.g., at a company like Gifthealth), the following controls apply:

### HIPAA Compliance Requirements

1. **Business Associate Agreements (BAAs)**
   - Required with all cloud vendors (Snowflake, AWS/GCP, Supabase)
   - Required with any BI tool vendor processing PHI
   - BAA must be executed before any data transfer

2. **Minimum Necessary Standard**
   - Analytics queries must access only the minimum data needed
   - Row-level security enforced via RBAC model (see rbac_model.md)
   - Column-level masking for PII fields in non-admin roles

3. **De-identification for Analytics**
   - Patient identifiers hashed before loading into analytics warehouse
   - Prescriber-level detail available only to authorized roles
   - Aggregated views for executive and partner reporting

4. **Audit Logging**
   - All data access logged with user, timestamp, query, and rows returned
   - Snowflake: `ENABLE_QUERY_HISTORY` + `ACCESS_HISTORY` view
   - PostgreSQL: `pgaudit` extension
   - Logs retained for minimum 6 years per HIPAA

5. **Encryption**
   - Data at rest: AES-256 (Snowflake default, S3 SSE)
   - Data in transit: TLS 1.2+ for all connections
   - Key management via cloud KMS (AWS KMS, GCP CMEK)

### Pharmacy-Specific Regulations

1. **DEA Schedule Drugs**
   - Controlled substance prescribing data requires additional access controls
   - Opioid prescribing flags in `mart_prescriber_engagement` are derived from
     CMS public aggregates, not individual patient records

2. **State Pharmacy Board Requirements**
   - Some states have stricter data handling rules for pharmacy data
   - State-level access controls may be needed for geographic analysis

3. **FDA Reporting**
   - Adverse event data (if collected) subject to FDA FAERS reporting
   - Not applicable to this project (CMS claims data only)

---

## Data Retention

| Data Type | Retention Period | Justification |
|---|---|---|
| Raw CMS PUF files | Indefinite | Public data, no retention limit |
| dbt staging models | Rebuilt on each run | Ephemeral views |
| dbt mart models | Current + 5 years historical | Trend analysis requirement |
| Snapshots (SCD Type 2) | Indefinite | Historical pricing record |
| Audit logs | 6 years minimum | HIPAA requirement |
| Processed CSVs (local) | 90 days | Development artifacts |

---

## Incident Response

If a data exposure is suspected:
1. Immediately revoke access to affected role(s)
2. Review audit logs for unauthorized access patterns
3. Notify Data & Analytics team lead within 1 hour
4. For HIPAA-covered data: follow Breach Notification Rule (45 CFR 164.400-414)
5. Document incident in governance log with root cause and remediation
