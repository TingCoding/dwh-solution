# DWH Coding Challenge - Solution

## Overview

This solution processes data warehouse event logs to:
1. Build historical views for accounts, cards, and saving_accounts tables
2. Create a denormalized joined timeline
3. Detect transactions (changes in balances and credit usage)

## Architecture

### Data Processing Flow
```
Event Logs (JSON/JSONL) 
  → Parse & Sort by Timestamp
  → Build Historical Views (versioned snapshots)
  → Join Tables Chronologically
  → Forward-Fill Missing Values
  → Detect Transactions
  → Output Analytics
```

### Key Design Decisions

**1. Event Processing**
- Supports both JSON arrays and JSON Lines format
- Handles create (`c`) and update (`u`) operations
- Sorts by timestamp with operation order (create before update)

**2. Historical View Construction**
- Maintains state dictionary per record ID
- Each event creates a new version
- Snapshot shows complete state after each event

**3. Denormalized Join**
- Time-series based join on account_id
- Forward-fill propagates last known values
- Auto-detects join keys (account_id, record_id variants)

**4. Transaction Detection**
- Compares consecutive values within record groups
- Tracks changes in saving_balance and card_credit_used
- Calculates deltas for each change

## Running the Solution

### Prerequisites
- Docker installed
- Data structure:
```
dwh-coding-challenge/
├── solution/
│   ├── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── build_and_run.sh
│   └── README.md
└── data/
    ├── accounts/
    ├── cards/
    └── saving_accounts/
```

## Syarat instalasi
install atau nyalakan docker desktop

### Option 1: Using Build Scripts

**Linux/Mac:**
```bash
cd solution
chmod +x build_and_run.sh
./build_and_run.sh
```

**Windows:**
```cmd
cd solution
.\build_and_run.bat
```

### Option 2: Manual Docker Commands

```bash
# Build image agar bisa build container
cd ..
docker build -t dwh-coding-challenge -f solution/Dockerfile .

# Run container agar muncul di docker local
docker run --name dwh-solution dwh-coding-challenge

# Cleanup atau hapus container ini di docker
docker rm dwh-solution
```

### Option 3: Local Execution

```bash
cd solution
pip install -r requirements.txt
python main.py
```

## Expected Output

### 1. Historical Views
Shows versioned snapshots for each table with all field changes over time.

### 2. Denormalized Joined View
Combined timeline of all three tables with forward-filled values, showing the complete state at each timestamp.

### 3. Transaction Detection
Lists all detected changes in:
- `saving_balance`: deposits/withdrawals in savings accounts
- `card_credit_used`: credit card usage changes

Includes summary statistics by transaction type and account.

## Implementation Details

### Algorithms

**Forward-Fill Join:**
```python
# Merge all tables chronologically
combined = pd.concat([accounts, cards, savings])
combined.sort_values(['account_id', 'ts'])
# Propagate last known values
combined.groupby('account_id').ffill()
```

**Transaction Detection:**
```python
# Track changes within each record
for record in grouped_data:
    for current, previous in zip(values[1:], values[:-1]):
        if current != previous:
            record_transaction(delta=current - previous)
```

### Data Quality Handling

- **Missing Values**: Forward-filled within account groups
- **Type Safety**: Numeric conversion with error handling
- **Timestamp Parsing**: Graceful coercion for invalid dates
- **Empty Data**: Defensive checks throughout

## File Structure

```
solution/
├── main.py              # Core solution (all-in-one)
├── Dockerfile          # Container definition
├── requirements.txt    # Python dependencies
├── build_and_run.sh   # Linux/Mac script
├── build_and_run.bat  # Windows script
├── .dockerignore      # Build optimization
└── README.md          # This file
```

## Troubleshooting

**Issue**: Data directory not found
- Ensure `data/` is in parent directory of `solution/`

**Issue**: No transactions detected
- Verify data contains changing values in balance/credit_used fields

**Issue**: Docker build fails
- Check Docker is running
- Verify internet connection for base image download

## Testing Coverage

Tested with:
- Multiple JSON/JSONL formats
- Sparse updates (partial field updates)
- Missing/empty tables
- Various join key naming conventions
- Large datasets (1000+ events)
- Edge cases (empty files, single events)

## Performance Considerations

For large-scale deployment:
- Partition data by date/time periods
- Implement incremental processing
- Use columnar storage (Parquet)
- Consider distributed processing (Spark/Dask)
- Cache intermediate results

## Future Enhancements

- Support delete operations
- Add data validation rules
- Implement unit tests
- Create visualization dashboard
- Export to databases
- Add quality metrics

## Author

Solution for DWH Coding Challenge Submission