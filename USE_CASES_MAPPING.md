# Use Cases to dbt Models Mapping

This document maps Thiqah's use case requirements to the dbt models that implement them.

## Use Case 1: DWH_BIAllProducts - Multi-Source User & Services Unification

### Requirements Summary
- Unify users & services from 17 data sources (POC uses 5 systems)
- Handle datatype drift (INT, STRING, GUID variations)
- Normalize inconsistent semantics (status flags, field names)
- Resolve conflicting enums and lookup values
- Deduplicate and perform identity resolution
- Apply survivorship rules
- Align with reference data

### Implementation

#### Seeds (Sample Data)
- `system_a_users.csv` - INT user_id, "active"/"disabled" status
- `system_b_users.csv` - STRING user_id with 'U' prefix, boolean is_active
- `system_c_users.csv` - GUID user_id, status codes "ACT"/"INA"
- `system_d_services.csv` - Services linked via user_id
- `system_e_services.csv` - Services linked via email
- `reference_service_catalog.csv` - Canonical service names

#### Macros (Reusable Transformations)
- `normalize_phone_number()` - Standardizes phone to 966XXXXXXXXX format
- `normalize_email()` - Lowercase, trim, validate
- `normalize_status()` - Maps active/enabled/1/true/ACT/Y → boolean
- `generate_surrogate_key()` - Creates consistent keys across systems

#### Staging Models (Type Coercion & Normalization)
- `stg_system_a_users` - Normalizes System A user data
- `stg_system_b_users` - Normalizes System B user data  
- `stg_system_c_users` - Normalizes System C user data
- `stg_system_d_services` - Normalizes System D services
- `stg_system_e_services` - Normalizes System E services
- `stg_reference_service_catalog` - Reference data

**Transformations:**
- ✅ Type coercion (INT/STRING/GUID → STRING)
- ✅ Field normalization (phone, email, case)
- ✅ Semantic mapping (status variants → is_active boolean)

#### Intermediate Models (Identity Resolution)
- `int_all_users_combined` - Unions all user sources with priority ranking
- `int_user_identity_clusters` - Groups users by email/phone matching
- `int_user_survivorship` - Applies survivorship rules to pick best values

**Transformations:**
- ✅ Survivorship rules (priority + freshness)
- ✅ Deduplication (global_user_id generation)
- ✅ Identity resolution (email + phone matching)

#### Mart Models (Analytics-Ready)
- `dim_unified_users` - Final unified user dimension (1 row per unique user)
- `fct_user_services` - Services fact table with user links

**Transformations:**
- ✅ Reference data alignment (service catalog)
- ✅ Cross-system user linking
- ✅ Service status tracking

### Deliverables
✅ Unified user dimension across 3 systems  
✅ Demonstrates type coercion for INT/STRING/GUID  
✅ Semantic mapping for multiple status representations  
✅ Survivorship logic based on source priority + freshness  
✅ Identity resolution and deduplication  
✅ Service catalog alignment  

---

## Use Case 2: PaymentGateway - Incremental Bills Load with Schema Enforcement

### Requirements Summary
- Enforce strict schema contract for SQL Server → GCS → BigQuery pipeline
- Prevent Parquet auto-inference type mismatches
- Preserve numeric types (FLOAT64 with decimals, no downcast to INT)
- Keep string types (reference IDs must not be converted to numeric)
- Normalize datetime to KSA timezone without TZ
- Handle null and sentinel values

### Implementation

#### Seeds (Sample Data)
- `payment_gateway_bills.csv` - Sample billing data with mixed types

#### Macros (Type Safety)
- `safe_cast_with_default()` - Safe casting with fallback values
- `normalize_datetime_to_ksa()` - Converts to KSA DATETIME (no TZ)

#### Staging Models (Schema Enforcement)
- `stg_bills` - Strict schema enforcement layer

**Transformations:**
- ✅ INT64 enforcement for bill_id (reject non-numeric)
- ✅ STRING preservation for billing_system_reference_no (no auto-conversion)
- ✅ DATETIME conversion to KSA timezone (strip TZ)
- ✅ FLOAT64 preservation for amounts (keep decimals)
- ✅ Data quality filtering (invalid records excluded)

#### Mart Models (Analytics-Ready)
- `fct_bills` - Bills fact table with date dimensions

**Additional Features:**
- ✅ VAT percentage calculation
- ✅ Date dimension fields (year, month, day, day_of_week)
- ✅ Payment status flags (is_paid, is_failed, is_pending)
- ✅ Days to payment calculation

### Target Schema Compliance

| Column | Required Type | Implementation | Status |
|--------|---------------|----------------|--------|
| bill_id | INT64 | `safe_cast_with_default('bill_id', 'INT64')` | ✅ |
| billing_system_reference_no | STRING | `cast(...as string)` - No auto-conversion | ✅ |
| payment_date | DATETIME | `normalize_datetime_to_ksa()` | ✅ |
| amount | FLOAT64 | `cast(...as float64)` - Preserves decimals | ✅ |
| vat | FLOAT64 | `cast(...as float64)` - Preserves decimals | ✅ |

### Deliverables
✅ Schema enforcement layer preventing Parquet issues  
✅ Type safety with safe casting  
✅ DATETIME normalization to KSA timezone  
✅ Numeric precision preservation  
✅ STRING preservation (no numeric coercion)  
✅ Data quality validation  

---

## Use Case 3: Halal DWH - JSON Fields to Relational Columns

### Requirements Summary
- Extract key attributes from NVARCHAR(JSON) columns
- Validate JSON structure
- Handle missing/renamed keys
- Route invalid JSON to Dead Letter Queue (DLQ)
- Extract nested paths ($.merchant.id, $.transaction.amount.value)
- Type assignment (STRING, DECIMAL, DATETIME)
- Default missing keys to NULL
- Control schema drift

### Implementation

#### Seeds (Sample Data)
- `halal_transactions_raw.csv` - Transactions with JSON data (includes invalid sample)

#### Macros (JSON Handling)
- `safe_json_extract()` - Safe JSON extraction with type casting and error handling

#### Staging Models (JSON Flattening)
- `stg_transactions_raw` - Extracts structured data from valid JSON
- `stg_transactions_dlq` - Dead Letter Queue for invalid JSON

**Transformations:**
- ✅ JSON validation (SAFE.PARSE_JSON)
- ✅ Path extraction:
  - `$.merchant.id` → merchant_id (STRING)
  - `$.merchant.name` → merchant_name (STRING)
  - `$.merchant.category` → merchant_category (STRING)
  - `$.transaction.amount.value` → transaction_amount (FLOAT64)
  - `$.transaction.currency` → currency (STRING)
  - `$.transaction.timestamp` → transaction_timestamp (DATETIME)
  - `$.certification.halal_certified` → is_halal_certified (BOOL)
  - `$.certification.certificate_id` → certificate_id (STRING)
  - `$.certification.expiry_date` → certificate_expiry_date (DATE)
- ✅ Type assignment for all extracted fields
- ✅ NULL defaults for missing keys
- ✅ DLQ routing for invalid JSON

#### Mart Models (Analytics-Ready)
- `fct_transactions` - Transactions fact table with flattened JSON

**Additional Features:**
- ✅ Certificate validity checking
- ✅ Days until expiry calculation
- ✅ Date dimension fields (year, month, day, hour)
- ✅ Merchant dimension fields

### DLQ Error Types

| Error Type | Description | Count in Sample Data |
|------------|-------------|----------------------|
| NULL_JSON | JSON field is NULL | 0 |
| EMPTY_JSON | JSON field is empty string | 0 |
| INVALID_JSON_FORMAT | Malformed JSON that can't be parsed | 1 |

### Deliverables
✅ JSON validation with DLQ routing  
✅ Nested path extraction (merchant, transaction, certification)  
✅ Type assignment (STRING, FLOAT64, BOOL, DATE)  
✅ NULL defaults for missing keys  
✅ Schema drift control (whitelist approach via macro)  
✅ Error capture and logging in DLQ  

---

## Cross-Cutting Features

### Data Quality
- Schema tests (not_null, unique, relationships, accepted_values)
- Custom business rule tests
- DLQ for invalid data
- Data quality flags (is_valid_record, is_valid_json)

### Audit Fields
- `_loaded_at` - When record was processed by dbt
- `_dbt_loaded_at` - When final mart was loaded

### Documentation
- Comprehensive column descriptions
- Test definitions
- Lineage via dbt docs
- Example queries in analyses/

### Macros (Reusability)
7 custom macros for common transformations:
1. `normalize_phone_number()` - Phone standardization
2. `normalize_email()` - Email standardization
3. `normalize_status()` - Boolean status mapping
4. `normalize_datetime_to_ksa()` - KSA timezone conversion
5. `safe_json_extract()` - JSON path extraction
6. `generate_surrogate_key()` - Consistent key generation
7. `safe_cast_with_default()` - Type casting with fallback

### Performance Considerations
- Views for staging (fast, no storage cost)
- Views for intermediate (composable, fresh)
- Tables for marts (performant queries, for BI)
- Partitioning/clustering can be added per model config

---

## Testing Strategy

### Schema Tests (via YAML)
- Uniqueness constraints
- Not null constraints
- Referential integrity
- Accepted values
- Expression tests (e.g., amount >= 0)

### Custom Tests (via tests/)
- Business rule validation
- Cross-table consistency
- Data freshness
- Orphaned records detection

### Test Execution
```bash
# Run all tests
dbtf test

# Test specific model
dbtf test --select dim_unified_users

# Test by use case tag
dbtf test --select tag:use_case_1
```

---

## Model Count Summary

| Layer | Use Case 1 | Use Case 2 | Use Case 3 | Total |
|-------|-----------|-----------|-----------|-------|
| Seeds | 6 | 1 | 1 | 8 |
| Staging | 6 | 1 | 2 | 9 |
| Intermediate | 3 | 0 | 0 | 3 |
| Marts | 2 | 1 | 1 | 4 |
| **Total** | **17** | **3** | **4** | **24** |

Plus: 7 macros, 8 seed schemas, comprehensive tests and documentation.

