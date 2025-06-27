# ETL Pipeline for Czech Parliament Data

This directory contains the ETL (Extract, Transform, Load) pipeline for processing Czech Parliament data from the official open data sources.

## Overview

The ETL system fetches data from the Czech Parliament's open data portal, processes it, and stores it in a PostgreSQL database. It handles MPs, voting records, bills, and other parliamentary data with support for both full synchronization and incremental updates.

## Core Components

### 1. `parliament_parser.py` - Data Fetching and Parsing

The parliament parser handles downloading and parsing Czech Parliament data from the official open data portal.

#### Key Classes

- **`UNLParser`**: Handles the Czech Parliament's UNL (Universal) data format
  - Supports Windows-1250 encoding
  - Handles pipe-delimited fields with escape sequences
  - Extracts ZIP files and decodes content

- **`ParliamentDataFetcher`**: Main interface for fetching parliamentary data
  - Fetches MPs, voting records, bills, and committee data
  - Supports selective data fetching for troubleshooting
  - Includes debugging and inspection capabilities

#### Usage Examples

```bash
# List available table schemas
python parliament_parser.py --action schemas

# Inspect ZIP file contents
python parliament_parser.py --action inspect --source poslanci.zip

# Fetch only specific tables
python parliament_parser.py --action fetch --source poslanci.zip --tables osoby

# Fetch only voting sessions (not individual votes)
python parliament_parser.py --action fetch --source hl-2021ps.zip --tables hl_hlasovani

# Run demo with sample data
python parliament_parser.py --action demo
```

#### Data Sources

- **`poslanci.zip`**: Current MPs and personal information
  - `osoby.unl`: Person records (names, birth dates, etc.)
  - `poslanec.unl`: MP records (contact info, constituency, etc.)

- **`hl-{period}.zip`**: Voting data by electoral period
  - `hl_hlasovani.unl`: Voting session records
  - `hl_poslanec.unl`: Individual MP votes

- **`tisky.zip`**: Parliamentary bills and documents
  - `tisk.unl`: Bill records

- **`organy.zip`**: Parliamentary committees and organs
  - `organ.unl`: Committee records

#### Key Methods

- `fetch_data_source(zip_filename, table_filters)`: Generic method to fetch any data source with optional filtering
- `fetch_mp_data(table_filters)`: Fetch MP and person data
- `fetch_voting_data(period, table_filters)`: Fetch voting data for specific period
- `fetch_bills_data(table_filters)`: Fetch bills data
- `inspect_zip_contents(zip_filename)`: Inspect ZIP contents without parsing
- `list_available_schemas()`: Get all defined table schemas

### 2. `etl_pipeline.py` - ETL Pipeline and Database Operations

The ETL pipeline orchestrates data synchronization, manages database connections, and provides monitoring capabilities.

#### Key Classes

- **`ETLPipeline`**: Main ETL orchestrator
  - Handles full and incremental synchronization
  - Manages data caching and change detection
  - Supports selective sync for troubleshooting

- **`ETLScheduler`**: Automated scheduling
  - Daily incremental sync at 6 AM
  - Weekly full sync on Sundays at 2 AM

- **`ETLMonitor`**: Health checking and monitoring
  - Data freshness checks
  - System health monitoring
  - Sync operation tracking

#### Usage Examples

```bash
# Full synchronization
python etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action full-sync

# Incremental sync (daily updates)
python etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action incremental-sync

# Monitor system health
python etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action monitor

# Test sync specific data source
python etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action test-sync --data-source mp --tables osoby

# Inspect data source
python etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action inspect --zip-file poslanci.zip

# Start automated scheduler
python etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action scheduler
```

#### Sync Methods

- `sync_persons_and_mps(table_filters)`: Sync person and MP data
- `sync_voting_data(period, table_filters)`: Sync voting data
- `sync_bills_data(table_filters)`: Sync bills data
- `sync_electoral_periods()`: Sync electoral period reference data
- `sync_parties()`: Sync political party reference data
- `sync_constituencies()`: Sync constituency reference data

#### Testing & Debugging Methods

- `run_test_sync(data_source, table_filters)`: Test specific data sources
- `inspect_data_source(zip_filename)`: Inspect data sources without processing
- `run_full_sync()`: Complete data synchronization
- `run_incremental_sync()`: Daily update synchronization

## Database Schema

The system uses PostgreSQL with the schema defined in `prover_poslance_schema.sql`:

### Core Tables
- `persons`: Person records (MPs, senators, etc.)
- `mps`: Member of Parliament records with contact info
- `voting_sessions`: Parliamentary voting sessions
- `vote_records`: Individual MP votes
- `bills`: Parliamentary bills and documents
- `committees`: Parliamentary committees and organs
- `parties`: Political parties
- `constituencies`: Electoral constituencies
- `electoral_periods`: Electoral periods

### Reference Data
- Static reference data (parties, constituencies, electoral periods)
- Maintained manually in code with predefined values

## Data Processing Flow

1. **Download**: Fetch ZIP files from Czech Parliament open data portal
2. **Extract**: Extract UNL files from ZIP archives
3. **Parse**: Parse UNL format with proper encoding (Windows-1250)
4. **Transform**: Convert dates, handle null values, map relationships
5. **Load**: Insert/update records in PostgreSQL database
6. **Cache**: Store raw data locally for incremental updates
7. **Monitor**: Track sync operations and data quality

## Troubleshooting Guide

### Common Issues

1. **Network Timeouts**: The parser includes retry logic for network requests
2. **Encoding Issues**: Uses Windows-1250 encoding for Czech characters
3. **Data Format Changes**: Logs warnings for unexpected field counts
4. **Database Conflicts**: Uses upsert operations (INSERT/UPDATE)

### Debugging Commands

```bash
# Test only person data sync
python etl_pipeline.py --database-url "..." --action test-sync --data-source mp --tables osoby

# Check what's in a ZIP file
python etl_pipeline.py --database-url "..." --action inspect --zip-file poslanci.zip

# Test voting sessions only (faster than full voting data)
python etl_pipeline.py --database-url "..." --action test-sync --data-source voting --tables hl_hlasovani

# Monitor recent sync operations
python etl_pipeline.py --database-url "..." --action monitor
```

### Log Files
- `etl_pipeline.log`: ETL operations and errors
- Console output: Real-time progress and status

### Data Caching
- `data_cache/`: Stores raw JSON data for incremental updates
- Helps avoid unnecessary API calls
- Used for change detection

## Configuration

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database schema
psql -d your_database -f prover_poslance_schema.sql
```

### Database URL Format
```
postgresql://username:password@hostname:port/database_name
```

### Scheduling
The system supports automated scheduling:
- **Daily Sync**: 6:00 AM (incremental updates)
- **Weekly Full Sync**: Sunday 2:00 AM (complete refresh)

## Data Sources Information

All data comes from the Czech Parliament's open data portal:
- **URL**: https://www.psp.cz/sqw/hp.sqw?k=1300
- **Format**: UNL (Universal) format in ZIP files
- **Encoding**: Windows-1250 (Czech-specific)
- **Update Frequency**: Daily
- **License**: Free to use with source attribution

## Czech-Specific Notes

### Data Codes
- **Vote Results**: A=ano/yes, N=ne/no, Z=zdržel se/abstain, @=nepřihlášen/not registered, M=omluven/excused, X=nehlasoval/did not vote, 0=nezaznamenán/not recorded
- **Gender Values**: M=muž/male, Ž=žena/female (note: Czech 'Ž' not 'F')
- **Date Format**: DD.MM.YYYY format from PSP, converted to ISO format

### Field Names
All field names preserve original Czech terminology for authenticity:
- `osoby` = persons
- `poslanec` = MP/deputy
- `hlasovani` = voting
- `tisk` = bill/document
- `organ` = committee/organ

## Performance Considerations

- **Connection Pooling**: Uses asyncpg connection pooling
- **Batch Processing**: Processes records in batches
- **Indexing**: Database includes performance indexes
- **Caching**: Local caching reduces API calls
- **Selective Sync**: Cherry-pick specific tables for faster testing

## Support

For issues or questions:
1. Check log files for error details
2. Use debug commands to isolate problems
3. Test specific data sources individually
4. Verify database connectivity and schema