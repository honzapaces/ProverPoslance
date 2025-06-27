# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ProverPoslance is a Czech Parliament data analysis application that provides transparent information about MPs and parliamentary activities. The system consists of an ETL pipeline that fetches, processes, and stores data from the Czech Parliament's open data portal available at: https://www.psp.cz/sqw/hp.sqw?k=1300

The data source provides:
- Daily updated ZIP files containing parliamentary data from 1993 to present
- UNL format files with Windows-1250 encoding and pipe (|) delimiters
- Free-to-use data requiring source attribution
- Comprehensive coverage of MPs, voting records, bills, and parliamentary sessions

## Architecture

### Core Components

- **ETL Pipeline** (`etl/etl_pipeline.py`): Main orchestrator for data synchronization
  - `ETLPipeline`: Handles full and incremental data synchronization
  - `ETLScheduler`: Automated scheduling for daily/weekly syncs
  - `ETLMonitor`: Health checking and monitoring
  - `DatabaseManager`: PostgreSQL connection pooling and management

- **Parliament Data Parser** (`etl/parliament_parser.py`): Handles Czech Parliament data formats
  - `ParliamentDataFetcher`: Downloads and caches data from PSP endpoints
  - `UNLParser`: Parses Czech Parliament's UNL format files

- **Database Schema** (`etl/prover_poslance_schema.sql`): PostgreSQL schema for Czech Parliament data
  - Core tables: `persons`, `mps`, `parties`, `constituencies`, `voting_sessions`, `vote_records`, `bills`
  - Materialized views for common queries: `v_current_mps`, `v_mp_voting_stats`
  - Full-text search capabilities using PostgreSQL's `pg_trgm` extension

### Data Flow

1. **Data Sources**: Czech Parliament Public APIs (psp.cz/eknih/cdrom/opendata/)
2. **Raw Data**: Downloaded as ZIP files containing UNL format data
3. **Processing**: Parsed and normalized by `parliament_parser.py`
4. **Storage**: PostgreSQL database with comprehensive schema
5. **Caching**: Local JSON files in `etl/data_cache/` for incremental updates

## Common Development Commands

### Database Setup
```bash
# Initialize database schema
psql -d your_database -f etl/prover_poslance_schema.sql
```

### ETL Operations
```bash
# Install dependencies
cd etl && pip install -r requirements.txt

# Run full data synchronization
python etl/etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action full-sync

# Run incremental sync (daily updates)
python etl/etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action incremental-sync

# Monitor system health
python etl/etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action monitor

# Start automated scheduler
python etl/etl_pipeline.py --database-url "postgresql://user:pass@host/db" --action scheduler
```

## Key Technical Details

### Data Sources
All data is sourced from the Czech Parliament's open data portal (updated daily):
- **MP Data**: `poslanci.zip` - Current MPs and personal information (`osoby.unl`, `poslanec.unl`)
- **Voting Data**: `hl-{period}.zip` - Individual voting records by electoral period (`hl_hlasovani.unl`, `hl_poslanec.unl`)
- **Bills Data**: `tisky.zip` - Parliamentary bills and documents (`tisk.unl`)
- **Committee Data**: `organy.zip` - Parliamentary committees and organs (`organ.unl`)
- **Sessions Data**: Various session-related files (`schuze.unl`, stenographic records)
- **Reference Data**: Electoral periods, constituencies, political parties (manually maintained in code)

### Data Synchronization Strategy
- **Full Sync**: Complete data refresh (weekly, Sundays at 2 AM)
- **Incremental Sync**: Updated records only (daily at 6 AM)
- **Caching**: Local JSON files prevent unnecessary API calls
- **Logging**: All sync operations logged to `data_sync_log` table

### Database Considerations
- Uses PostgreSQL-specific features (full-text search, generated columns, array types)
- Connection pooling with asyncpg for concurrent operations
- Proper indexing for performance on large datasets (200+ MPs, 100k+ votes)
- Foreign key constraints maintain data integrity

### Error Handling
- Comprehensive logging to both file (`etl_pipeline.log`) and console
- Graceful handling of network failures and data format changes
- Failed records tracked separately without stopping entire sync
- Health monitoring detects stale data and system issues

## Important Notes

### Data Format Specifics
- **Character Encoding**: Windows-1250 (Czech-specific encoding)
- **File Format**: UNL (Universal data format) with pipe (|) delimiters
- **Escape Sequences**: Backslash (\) escapes special characters
- **Null Values**: Empty fields between delimiters represent NULL values
- **Date Formats**: DD.MM.YYYY format from PSP, converted to ISO format for database

### Czech Parliament Data Codes
- **Vote Results**: A=ano/yes, N=ne/no, Z=zdržel se/abstain, @=nepřihlášen/not registered, M=omluven/excused, X=nehlasoval/did not vote, 0=nezaznamenán/not recorded
- **Gender Values**: M=muž/male, Ž=žena/female (note: uses Czech 'Ž' not 'F')
- **Electoral Periods**: Numbered sequentially (9th period = 2021-present)

### Data Maintenance
- Party affiliations, colors, and constituency mappings are manually maintained in code
- Reference data updates require code changes and redeployment
- All field names and values preserve original Czech terminology for authenticity
- Database schema includes Czech comments for field explanations