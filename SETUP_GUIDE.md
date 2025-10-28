# Thiqah Analytics dbt Project - Setup Guide

## Quick Start for POC Demo

This guide will help you set up and run the dbt project for the Thiqah analytics POC.

### Prerequisites

1. **Python 3.8+** installed
2. **GCP Project** with BigQuery enabled
3. **Service Account** with BigQuery permissions (Data Editor, Job User)

### Installation Steps

#### 1. Install dbt-bigquery

```bash
pip install dbt-bigquery
```

#### 2. Configure Your Profile

Move the `profiles.yml` file to your home directory:

```bash
mkdir -p ~/.dbt
cp profiles.yml ~/.dbt/profiles.yml
```

Edit `~/.dbt/profiles.yml` and update:
- `project`: Your GCP project ID
- `dataset`: Your target BigQuery dataset name
- `location`: Your GCP region (e.g., `europe-west2` or `us-central1`)
- `keyfile`: Path to your service account JSON key (for prod)

For development, you can use OAuth:
```yaml
thiqah_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: your-project-id
      dataset: analytics_dev
      threads: 4
      location: europe-west2
```

#### 3. Install dbt Packages

```bash
dbtf deps
```

#### 4. Test Your Connection

```bash
dbtf debug
```

You should see all connection tests pass.

### Running the Project

#### Load Seed Data

Load the sample source data into BigQuery:

```bash
dbtf seed
```

This creates tables in the `seeds` schema with sample data for:
- 3 user systems (A, B, C)
- 2 service systems (D, E)
- Payment gateway bills
- Halal certification transactions
- Reference service catalog

#### Build All Models

```bash
dbtf run
```

This will:
1. Create staging views (type coercion, normalization)
2. Create intermediate views (identity resolution, deduplication)
3. Create mart tables (analytics-ready dimensions and facts)

Expected output: ~20 models built successfully

#### Run Data Quality Tests

```bash
dbtf test
```

This validates:
- Unique keys
- Not null constraints
- Referential integrity
- Accepted values
- Custom business rules

#### Generate Documentation

```bash
dbtf docs generate
dbtf docs serve
```

This opens a browser with interactive documentation showing:
- Model lineage (DAG)
- Column descriptions
- Test results
- Source data

### Project Structure

```
models/
├── staging/              # Raw data cleaning
│   ├── biallproducts/   # Use Case 1: Multi-source
│   ├── payment_gateway/ # Use Case 2: Schema enforcement
│   └── halal/           # Use Case 3: JSON flattening
│
├── intermediate/        # Business logic
│   └── unified_users/   # Identity resolution
│
└── marts/              # Analytics-ready
    ├── core/           # Unified users & services
    ├── payments/       # Billing facts
    └── halal/          # Transaction facts
```

### Key Commands Reference

```bash
# Run specific model
dbtf run --select model_name

# Run all models in a directory
dbtf run --select staging.biallproducts.*

# Run model and all downstream dependencies
dbtf run --select model_name+

# Run model and all upstream dependencies
dbtf run --select +model_name

# Run models by tag
dbtf run --select tag:use_case_1

# Test specific model
dbtf test --select model_name

# Full refresh (for incremental models)
dbtf run --full-refresh

# Compile only (no execution)
dbtf compile
```

### Verification Queries

After running the project, verify the results:

```sql
-- Check unified users count
SELECT count(*) FROM `your-project.analytics.dim_unified_users`;

-- Check multi-source users
SELECT source_record_count, count(*) 
FROM `your-project.analytics.dim_unified_users`
GROUP BY source_record_count;

-- Check payment bills
SELECT payment_status, count(*), sum(total_amount)
FROM `your-project.analytics.fct_bills`
GROUP BY payment_status;

-- Check halal transactions
SELECT is_halal_certified, count(*), sum(transaction_amount)
FROM `your-project.analytics.fct_transactions`
GROUP BY is_halal_certified;

-- Check DLQ for invalid JSON
SELECT * FROM `your-project.analytics_staging.stg_transactions_dlq`;
```

### Troubleshooting

#### Connection Issues

If `dbtf debug` fails:
1. Check your GCP credentials
2. Verify BigQuery API is enabled
3. Confirm service account permissions
4. Check project ID is correct

#### Compilation Errors

If models fail to compile:
1. Check Jinja syntax
2. Verify all ref() and var() calls
3. Review macro definitions

#### Test Failures

If tests fail:
1. Review test results: `target/run_results.json`
2. Check data in seeds
3. Review business logic in models

#### Performance Issues

If queries are slow:
1. Check table sizes in BigQuery
2. Review materialization strategy (view vs table)
3. Consider adding partitioning/clustering
4. Increase threads in profile

### Next Steps

1. **Customize for Production**:
   - Update connection profiles
   - Add incremental loading logic
   - Configure CI/CD pipeline
   - Set up freshness checks

2. **Extend the Project**:
   - Add more source systems
   - Create additional marts
   - Build dashboard datasets
   - Add ML feature stores

3. **Governance**:
   - Define data ownership
   - Document business glossary
   - Set up data quality monitoring
   - Configure alerting

### Support

For questions or issues:
- Review documentation: `dbtf docs serve`
- Check dbt logs: `logs/dbt.log`
- Review compiled SQL: `target/compiled/`

## Demo Walkthrough

### Use Case 1: Multi-Source Unification

Show how 3 different user systems with different schemas are unified:

```bash
# Show raw seeds
dbtf seed --select system_a_users system_b_users system_c_users

# Show staging layer (normalization)
dbtf run --select stg_system_a_users stg_system_b_users stg_system_c_users

# Show intermediate layer (identity resolution)
dbtf run --select int_all_users_combined int_user_identity_clusters int_user_survivorship

# Show final mart
dbtf run --select dim_unified_users

# Query results
dbtf run-operation query --args '{query: "SELECT * FROM {{ ref(\"dim_unified_users\") }} LIMIT 10"}'
```

### Use Case 2: Schema Enforcement

Show strict type enforcement for payment data:

```bash
# Show seed with mixed types
dbtf seed --select payment_gateway_bills

# Show staging with schema enforcement
dbtf run --select stg_bills

# Show final mart
dbtf run --select fct_bills

# Run data quality tests
dbtf test --select fct_bills
```

### Use Case 3: JSON Flattening

Show JSON extraction from NVARCHAR columns:

```bash
# Show raw JSON data
dbtf seed --select halal_transactions_raw

# Show JSON extraction (valid records)
dbtf run --select stg_transactions_raw

# Show DLQ (invalid JSON)
dbtf run --select stg_transactions_dlq

# Show final mart
dbtf run --select fct_transactions
```

