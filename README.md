# Thiqah Analytics - dbt Project

## Overview
This dbt project implements the data transformation layer for Thiqah's modern data platform on GCP BigQuery, addressing three primary use cases:

1. **Multi-Source User & Services Unification** - Consolidating 17 heterogeneous data sources
2. **Payment Gateway Incremental Loads** - Schema enforcement and type safety for billing data
3. **Halal DWH JSON Flattening** - Extracting structured data from JSON columns

## Project Structure

```
thiqah_analytics/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt package dependencies
├── profiles.yml             # Connection profiles (move to ~/.dbt/)
│
├── seeds/                   # Sample source data
│   └── source_systems/      # Representing different source systems
│
├── models/
│   ├── staging/             # Raw data cleaning & type casting
│   │   ├── biallproducts/   # Use Case 1: Multi-source unification
│   │   ├── payment_gateway/ # Use Case 2: Payment data
│   │   └── halal/           # Use Case 3: JSON flattening
│   │
│   ├── intermediate/        # Business logic transformations
│   │   ├── unified_users/   # Identity resolution
│   │   └── payment_processing/
│   │
│   └── marts/               # Analytics-ready tables
│       ├── core/            # Unified user & services
│       ├── payments/        # Payment analytics
│       └── halal/           # Halal certification analytics
│
├── macros/                  # Reusable SQL functions
├── tests/                   # Custom data tests
└── analyses/                # Ad-hoc analyses

```

## Use Cases Implemented

### Use Case 1: Multi-Source Unification (DWH_BIAllProducts)
**Goal**: Unify users & services from 17 data sources

**Transformations**:
- Type coercion and normalization
- Semantic mapping of status fields
- Survivorship rules (pick best value by priority/freshness)
- Identity resolution and deduplication
- Reference data alignment

**Models**:
- `staging/biallproducts/stg_*` - One model per source system
- `intermediate/unified_users/int_*` - Deduplication and survivorship
- `marts/core/dim_unified_users` - Final unified user dimension

### Use Case 2: Payment Gateway (Incremental Loads)
**Goal**: Enforce schema contracts for SQL Server → GCS → BigQuery pipeline

**Transformations**:
- Strict schema enforcement
- Safe type casting
- DateTime normalization to KSA timezone
- Numeric precision preservation
- String preservation for reference IDs

**Models**:
- `staging/payment_gateway/stg_bills` - Schema enforcement layer
- `marts/payments/fct_bills` - Analytics-ready fact table

### Use Case 3: Halal DWH (JSON Flattening)
**Goal**: Extract structured data from JSON columns

**Transformations**:
- JSON validation
- Path extraction and type assignment
- Error handling (DLQ for invalid JSON)
- Schema drift control

**Models**:
- `staging/halal/stg_transactions_raw` - JSON extraction
- `marts/halal/fct_transactions` - Flattened transaction facts

## Getting Started

### Prerequisites
- Python 3.8+
- dbt-bigquery adapter
- GCP project with BigQuery enabled

### Installation

1. Install dbt:
```bash
pip install dbt-bigquery
```

2. Install dbt packages:
```bash
dbtf deps
```

3. Configure your profile:
   - Move `profiles.yml` to `~/.dbt/` directory
   - Update with your GCP project details

4. Test connection:
```bash
dbtf debug
```

### Running the Project

```bash
# Load seed data
dbtf seed

# Run all models
dbtf run

# Run tests
dbtf test

# Generate documentation
dbtf docs generate
dbtf docs serve
```

## Configuration

### Variables (dbt_project.yml)
- `timezone`: Asia/Riyadh (KSA timezone)
- `session_timeout_minutes`: 30 (configurable per model)
- `source_priority`: Priority ranking for survivorship logic

### Schemas
- `seeds`: Source seed data
- `staging`: Cleaned and typed raw data
- `intermediate`: Business logic transformations
- `marts`: Analytics-ready tables
- `test_failures`: Failed test results storage

## Development Workflow

1. Create staging models for new sources
2. Apply transformations in intermediate layer
3. Build final marts for analytics
4. Add data quality tests
5. Document models with descriptions
6. Review and deploy

## Key Macros

- `normalize_phone_number()` - Standardize phone formats
- `normalize_email()` - Standardize email formats
- `apply_survivorship()` - Implement survivorship rules
- `safe_json_extract()` - Safe JSON extraction with error handling
- `normalize_datetime_to_ksa()` - Convert timestamps to KSA timezone

## Testing Strategy

- **Schema tests**: Not null, unique, relationships
- **Data quality tests**: Custom business rules
- **Freshness tests**: Data recency checks
- **Custom tests**: Domain-specific validations

## Notes

- Use `dbtf` command instead of `dbt` for this project
- Session timeout is configurable per model
- Data quality checks can be toggled via variables

