import asyncio
import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager
import os
from pathlib import Path
import json
import hashlib
from enum import Enum

from parliament_parser import ParliamentDataFetcher, UNLParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SyncStatus(Enum):
    RUNNING = "running"
    COMPLETED = "completed" 
    FAILED = "failed"

@dataclass
class SyncResult:
    sync_type: str
    file_name: str
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    status: SyncStatus = SyncStatus.RUNNING
    error_message: Optional[str] = None
    started_at: datetime = None
    completed_at: Optional[datetime] = None

class DatabaseManager:
    """Handles database connections and operations"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None
    
    async def initialize(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60
        )
        logger.info("Database connection pool initialized")
    
    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool"""
        async with self.pool.acquire() as conn:
            yield conn

class ETLPipeline:
    """Main ETL pipeline for Czech Parliament data"""
    
    def __init__(self, database_url: str):
        self.db = DatabaseManager(database_url)
        self.fetcher = ParliamentDataFetcher()
        self.cache_dir = Path("./data_cache")
        self.cache_dir.mkdir(exist_ok=True)
        
    async def initialize(self):
        """Initialize the pipeline"""
        await self.db.initialize()
        logger.info("ETL Pipeline initialized")
    
    async def shutdown(self):
        """Shutdown the pipeline"""
        await self.db.close()
        logger.info("ETL Pipeline shutdown")
    
    async def log_sync_start(self, sync_type: str, file_name: str = "") -> int:
        """Log the start of a sync operation"""
        async with self.db.get_connection() as conn:
            sync_id = await conn.fetchval("""
                INSERT INTO data_sync_log (sync_type, file_name, sync_status, started_at)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """, sync_type, file_name, SyncStatus.RUNNING.value, datetime.now())
            
        logger.info(f"Started sync {sync_id}: {sync_type} - {file_name}")
        return sync_id
    
    async def log_sync_complete(self, sync_id: int, result: SyncResult):
        """Log the completion of a sync operation"""
        async with self.db.get_connection() as conn:
            await conn.execute("""
                UPDATE data_sync_log 
                SET records_processed = $1, records_inserted = $2, records_updated = $3,
                    records_failed = $4, sync_status = $5, error_message = $6, completed_at = $7
                WHERE id = $8
            """, result.records_processed, result.records_inserted, result.records_updated,
                result.records_failed, result.status.value, result.error_message, 
                datetime.now(), sync_id)
        
        logger.info(f"Completed sync {sync_id}: {result.status.value}")
    
    def calculate_data_hash(self, data: Any) -> str:
        """Calculate hash of data for change detection"""
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    async def cache_data(self, cache_key: str, data: Any):
        """Cache data to filesystem"""
        cache_file = self.cache_dir / f"{cache_key}.json"
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    async def load_cached_data(self, cache_key: str) -> Optional[Any]:
        """Load data from cache"""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    async def sync_electoral_periods(self) -> SyncResult:
        """Sync electoral periods data"""
        sync_id = await self.log_sync_start("electoral_periods")
        result = SyncResult("electoral_periods", "manual")
        
        try:
            # Define electoral periods (this is relatively static data)
            periods = [
                (9, '2021-10-08', None, '9. volební období (2021-)', True),
                (8, '2017-10-20', '2021-10-07', '8. volební období (2017-2021)', False),
                (7, '2013-10-25', '2017-10-19', '7. volební období (2013-2017)', False),
                (6, '2010-05-29', '2013-10-24', '6. volební období (2010-2013)', False),
                (5, '2006-06-02', '2010-05-28', '5. volební období (2006-2010)', False),
                (4, '2002-06-14', '2006-06-01', '4. volební období (2002-2006)', False),
                (3, '1998-06-19', '2002-06-13', '3. volební období (1998-2002)', False),
                (2, '1996-05-31', '1998-06-18', '2. volební období (1996-1998)', False),
                (1, '1993-12-01', '1996-05-30', '1. volební období (1993-1996)', False)
            ]
            
            async with self.db.get_connection() as conn:
                for period_num, start_date, end_date, description, is_active in periods:
                    result.records_processed += 1
                    
                    # Check if exists
                    existing = await conn.fetchval(
                        "SELECT id FROM electoral_periods WHERE period_number = $1",
                        period_num
                    )
                    
                    if existing:
                        # Update
                        await conn.execute("""
                            UPDATE electoral_periods 
                            SET start_date = $1, end_date = $2, description = $3, 
                                is_active = $4, updated_at = $5
                            WHERE period_number = $6
                        """, start_date, end_date, description, is_active, 
                            datetime.now(), period_num)
                        result.records_updated += 1
                    else:
                        # Insert
                        await conn.execute("""
                            INSERT INTO electoral_periods 
                            (period_number, start_date, end_date, description, is_active)
                            VALUES ($1, $2, $3, $4, $5)
                        """, period_num, start_date, end_date, description, is_active)
                        result.records_inserted += 1
            
            result.status = SyncStatus.COMPLETED
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Error syncing electoral periods: {e}")
        
        await self.log_sync_complete(sync_id, result)
        return result
    
    async def sync_parties(self) -> SyncResult:
        """Sync political parties data"""
        sync_id = await self.log_sync_start("parties")
        result = SyncResult("parties", "manual")
        
        try:
            # Define parties with colors (this could be expanded to fetch from external source)
            parties_data = [
                ('Občanská demokratická strana', 'ODS', '#0066CC'),
                ('ANO 2011', 'ANO', '#EC407A'),
                ('Piráti', 'Piráti', '#000000'),
                ('Svoboda a přímá demokracie', 'SPD', '#DC2626'),
                ('Starostové a nezávislí', 'STAN', '#FFA500'),
                ('Komunistická strana Čech a Moravy', 'KSČM', '#B91C1C'),
                ('Česká strana sociálně demokratická', 'ČSSD', '#F97316'),
                ('TOP 09', 'TOP 09', '#8B5CF6'),
                ('Křesťanská a demokratická unie - Československá strana lidová', 'KDU-ČSL', '#10B981'),
                ('Trikolóra hnutí občanů', 'Trikolóra', '#FF6B6B'),
                ('Přísaha', 'Přísaha', '#4ECDC4'),
                ('SPOLU', 'SPOLU', '#45B7D1')
            ]
            
            async with self.db.get_connection() as conn:
                for name, short_name, color in parties_data:
                    result.records_processed += 1
                    
                    existing = await conn.fetchval(
                        "SELECT id FROM parties WHERE short_name = $1", short_name
                    )
                    
                    if existing:
                        await conn.execute("""
                            UPDATE parties 
                            SET name = $1, color_hex = $2, updated_at = $3
                            WHERE short_name = $4
                        """, name, color, datetime.now(), short_name)
                        result.records_updated += 1
                    else:
                        await conn.execute("""
                            INSERT INTO parties (name, short_name, color_hex, is_active)
                            VALUES ($1, $2, $3, $4)
                        """, name, short_name, color, True)
                        result.records_inserted += 1
            
            result.status = SyncStatus.COMPLETED
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Error syncing parties: {e}")
        
        await self.log_sync_complete(sync_id, result)
        return result
    
    async def sync_constituencies(self) -> SyncResult:
        """Sync constituencies data"""
        sync_id = await self.log_sync_start("constituencies")
        result = SyncResult("constituencies", "manual")
        
        try:
            constituencies_data = [
                ('Praha', 'PHA', 'Praha'),
                ('Jihočeský kraj', 'JCK', 'České Budějovice'),
                ('Jihomoravský kraj', 'JMK', 'Brno'),
                ('Karlovarský kraj', 'KVK', 'Karlovy Vary'),
                ('Královéhradecký kraj', 'KHK', 'Hradec Králové'),
                ('Liberecký kraj', 'LBK', 'Liberec'),
                ('Moravskoslezský kraj', 'MSK', 'Ostrava'),
                ('Olomoucký kraj', 'OLK', 'Olomouc'),
                ('Pardubický kraj', 'PAK', 'Pardubice'),
                ('Plzeňský kraj', 'PLK', 'Plzeň'),
                ('Středočeský kraj', 'STK', 'Praha'),
                ('Ústecký kraj', 'ULK', 'Ústí nad Labem'),
                ('Vysočina', 'VYS', 'Jihlava'),
                ('Zlínský kraj', 'ZLK', 'Zlín')
            ]
            
            async with self.db.get_connection() as conn:
                for name, code, region in constituencies_data:
                    result.records_processed += 1
                    
                    existing = await conn.fetchval(
                        "SELECT id FROM constituencies WHERE code = $1", code
                    )
                    
                    if not existing:
                        await conn.execute("""
                            INSERT INTO constituencies (name, code, region)
                            VALUES ($1, $2, $3)
                        """, name, code, region)
                        result.records_inserted += 1
                    else:
                        result.records_updated += 1  # No actual update needed for static data
            
            result.status = SyncStatus.COMPLETED
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Error syncing constituencies: {e}")
        
        await self.log_sync_complete(sync_id, result)
        return result
    
    async def sync_persons_and_mps(self, table_filters: Optional[List[str]] = None) -> SyncResult:
        """Sync persons and MPs data from PSP
        
        Args:
            table_filters: Optional list of specific tables to sync (e.g., ['osoby'] for persons only)
        """
        sync_id = await self.log_sync_start("persons_mps", "poslanci.zip")
        result = SyncResult("persons_mps", "poslanci.zip")
        
        try:
            # Fetch MP data with optional filtering
            logger.info("Fetching MP data from PSP...")
            if table_filters:
                logger.info(f"Filtering tables: {table_filters}")
            
            mp_data = self.fetcher.fetch_mp_data(table_filters)
            
            # Cache the raw data
            await self.cache_data("latest_mp_data", mp_data)
            
            async with self.db.get_connection() as conn:
                # Sync persons first
                if 'osoby' in mp_data:
                    for person_record in mp_data['osoby']:
                        result.records_processed += 1
                        
                        try:
                            person_id = int(person_record['id_osoba'])
                            
                            # Parse dates
                            birth_date = None
                            death_date = None
                            
                            if person_record.get('narozeni'):
                                try:
                                    birth_date = datetime.strptime(person_record['narozeni'], '%d.%m.%Y').date()
                                except ValueError:
                                    pass
                            
                            if person_record.get('umrti'):
                                try:
                                    death_date = datetime.strptime(person_record['umrti'], '%d.%m.%Y').date()
                                except ValueError:
                                    pass
                            
                            # Check if person exists
                            existing = await conn.fetchval(
                                "SELECT id FROM persons WHERE id = $1", person_id
                            )
                            
                            if existing:
                                # Update person
                                await conn.execute("""
                                    UPDATE persons 
                                    SET title_before = $1, first_name = $2, last_name = $3,
                                        title_after = $4, birth_date = $5, death_date = $6,
                                        gender = $7, updated_at = $8
                                    WHERE id = $9
                                """, person_record.get('pred'), person_record.get('jmeno'),
                                    person_record.get('prijmeni'), person_record.get('za'),
                                    birth_date, death_date, person_record.get('pohlavi'),
                                    datetime.now(), person_id)
                                result.records_updated += 1
                            else:
                                # Insert person
                                await conn.execute("""
                                    INSERT INTO persons 
                                    (id, title_before, first_name, last_name, title_after,
                                     birth_date, death_date, gender)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                """, person_id, person_record.get('pred'), person_record.get('jmeno'),
                                    person_record.get('prijmeni'), person_record.get('za'),
                                    birth_date, death_date, person_record.get('pohlavi'))
                                result.records_inserted += 1
                        
                        except Exception as e:
                            logger.error(f"Error processing person {person_record}: {e}")
                            result.records_failed += 1
                
                # Sync MPs
                if 'poslanec' in mp_data:
                    for mp_record in mp_data['poslanec']:
                        try:
                            mp_id = int(mp_record['id_poslanec'])
                            person_id = int(mp_record['id_osoba'])
                            
                            # Determine constituency and party (this would need mapping logic)
                            constituency_id = None
                            party_id = None
                            
                            # For now, use current electoral period (9)
                            electoral_period_id = 9
                            
                            # Check if MP exists
                            existing = await conn.fetchval(
                                "SELECT id FROM mps WHERE id = $1", mp_id
                            )
                            
                            if existing:
                                # Update MP
                                await conn.execute("""
                                    UPDATE mps 
                                    SET person_id = $1, constituency_id = $2, party_id = $3,
                                        email = $4, phone = $5, office_phone = $6, fax = $7,
                                        website = $8, facebook = $9, street = $10, city = $11,
                                        postal_code = $12, photo_url = $13, updated_at = $14
                                    WHERE id = $15
                                """, person_id, constituency_id, party_id,
                                    mp_record.get('email'), mp_record.get('telefon'),
                                    mp_record.get('psp_telefon'), mp_record.get('fax'),
                                    mp_record.get('web'), mp_record.get('facebook'),
                                    mp_record.get('ulice'), mp_record.get('obec'),
                                    mp_record.get('psc'), mp_record.get('foto'),
                                    datetime.now(), mp_id)
                            else:
                                # Insert MP
                                await conn.execute("""
                                    INSERT INTO mps 
                                    (id, person_id, constituency_id, party_id, electoral_period_id,
                                     email, phone, office_phone, fax, website, facebook,
                                     street, city, postal_code, photo_url, is_active)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                                """, mp_id, person_id, constituency_id, party_id, electoral_period_id,
                                    mp_record.get('email'), mp_record.get('telefon'),
                                    mp_record.get('psp_telefon'), mp_record.get('fax'),
                                    mp_record.get('web'), mp_record.get('facebook'),
                                    mp_record.get('ulice'), mp_record.get('obec'),
                                    mp_record.get('psc'), mp_record.get('foto'), True)
                                result.records_inserted += 1
                        
                        except Exception as e:
                            logger.error(f"Error processing MP {mp_record}: {e}")
                            result.records_failed += 1
            
            result.status = SyncStatus.COMPLETED
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Error syncing persons and MPs: {e}")
        
        await self.log_sync_complete(sync_id, result)
        return result
    
    async def sync_voting_data(self, period: str = "hl-2021ps", table_filters: Optional[List[str]] = None) -> SyncResult:
        """Sync voting data for a specific electoral period
        
        Args:
            period: Electoral period identifier (e.g., 'hl-2021ps')
            table_filters: Optional list of specific tables to sync (e.g., ['hl_hlasovani'] for sessions only)
        """
        sync_id = await self.log_sync_start("voting_data", f"{period}.zip")
        result = SyncResult("voting_data", f"{period}.zip")
        
        try:
            logger.info(f"Fetching voting data for period {period}...")
            if table_filters:
                logger.info(f"Filtering tables: {table_filters}")
            
            voting_data = self.fetcher.fetch_voting_data(period, table_filters)
            
            # Cache the raw data
            await self.cache_data(f"latest_voting_data_{period}", voting_data)
            
            async with self.db.get_connection() as conn:
                # Sync voting sessions first
                if 'hl_hlasovani' in voting_data:
                    for session_record in voting_data['hl_hlasovani']:
                        result.records_processed += 1
                        
                        try:
                            session_id = int(session_record['id_hlasovani'])
                            
                            # Parse date and time
                            vote_date = None
                            vote_time = None
                            
                            if session_record.get('datum'):
                                try:
                                    vote_date = datetime.strptime(session_record['datum'], '%Y-%m-%d').date()
                                except ValueError:
                                    pass
                            
                            if session_record.get('cas'):
                                try:
                                    vote_time = datetime.strptime(session_record['cas'], '%H:%M:%S').time()
                                except ValueError:
                                    pass
                            
                            # Check if session exists
                            existing = await conn.fetchval(
                                "SELECT id FROM voting_sessions WHERE id = $1", session_id
                            )
                            
                            if existing:
                                # Update session
                                await conn.execute("""
                                    UPDATE voting_sessions 
                                    SET vote_date = $1, vote_time = $2, votes_for = $3,
                                        votes_against = $4, abstentions = $5, did_not_vote = $6,
                                        present_count = $7, quorum_met = $8, vote_type = $9,
                                        result = $10, title_long = $11, title_short = $12
                                    WHERE id = $13
                                """, vote_date, vote_time, 
                                    int(session_record.get('pro', 0)),
                                    int(session_record.get('proti', 0)),
                                    int(session_record.get('zdrzel', 0)),
                                    int(session_record.get('nehlasoval', 0)),
                                    int(session_record.get('prihlaseno', 0)),
                                    bool(session_record.get('kvorum')),
                                    session_record.get('druh_hlasovani'),
                                    session_record.get('vysledek'),
                                    session_record.get('nazev_dlouhy'),
                                    session_record.get('nazev_kratky'),
                                    session_id)
                                result.records_updated += 1
                            else:
                                # Insert session (need to handle committee_id mapping)
                                committee_id = int(session_record.get('id_organ', 165))  # Default to main parliament
                                
                                await conn.execute("""
                                    INSERT INTO voting_sessions 
                                    (id, committee_id, session_number, vote_number, agenda_item,
                                     vote_date, vote_time, votes_for, votes_against, abstentions,
                                     did_not_vote, present_count, quorum_met, vote_type, result,
                                     title_long, title_short)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                                """, session_id, committee_id,
                                    int(session_record.get('schuze', 0)),
                                    int(session_record.get('cislo', 0)),
                                    int(session_record.get('bod', 0)),
                                    vote_date, vote_time,
                                    int(session_record.get('pro', 0)),
                                    int(session_record.get('proti', 0)),
                                    int(session_record.get('zdrzel', 0)),
                                    int(session_record.get('nehlasoval', 0)),
                                    int(session_record.get('prihlaseno', 0)),
                                    bool(session_record.get('kvorum')),
                                    session_record.get('druh_hlasovani'),
                                    session_record.get('vysledek'),
                                    session_record.get('nazev_dlouhy'),
                                    session_record.get('nazev_kratky'))
                                result.records_inserted += 1
                        
                        except Exception as e:
                            logger.error(f"Error processing voting session {session_record}: {e}")
                            result.records_failed += 1
                
                # Sync individual votes
                if 'hl_poslanec' in voting_data:
                    for vote_record in voting_data['hl_poslanec']:
                        try:
                            voting_session_id = int(vote_record['id_hlasovani'])
                            mp_id = int(vote_record['id_poslanec'])
                            vote_result = vote_record['vysledek']
                            
                            # Check if vote record exists
                            existing = await conn.fetchval("""
                                SELECT id FROM vote_records 
                                WHERE voting_session_id = $1 AND mp_id = $2
                            """, voting_session_id, mp_id)
                            
                            if existing:
                                # Update vote
                                await conn.execute("""
                                    UPDATE vote_records 
                                    SET vote_result = $1
                                    WHERE voting_session_id = $2 AND mp_id = $3
                                """, vote_result, voting_session_id, mp_id)
                            else:
                                # Insert vote
                                await conn.execute("""
                                    INSERT INTO vote_records (voting_session_id, mp_id, vote_result)
                                    VALUES ($1, $2, $3)
                                """, voting_session_id, mp_id, vote_result)
                                result.records_inserted += 1
                        
                        except Exception as e:
                            logger.error(f"Error processing vote record {vote_record}: {e}")
                            result.records_failed += 1
            
            result.status = SyncStatus.COMPLETED
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Error syncing voting data: {e}")
        
        await self.log_sync_complete(sync_id, result)
        return result
    
    async def sync_bills_data(self, table_filters: Optional[List[str]] = None) -> SyncResult:
        """Sync bills and documents data
        
        Args:
            table_filters: Optional list of specific tables to sync (e.g., ['tisk'] for bills only)
        """
        sync_id = await self.log_sync_start("bills", "tisky.zip")
        result = SyncResult("bills", "tisky.zip")
        
        try:
            logger.info("Fetching bills data...")
            if table_filters:
                logger.info(f"Filtering tables: {table_filters}")
            
            bills_data = self.fetcher.fetch_bills_data(table_filters)
            
            # Cache the raw data
            await self.cache_data("latest_bills_data", bills_data)
            
            async with self.db.get_connection() as conn:
                if 'tisk' in bills_data:
                    for bill_record in bills_data['tisk']:
                        result.records_processed += 1
                        
                        try:
                            bill_id = int(bill_record['id_tisk'])
                            
                            # Parse date
                            submitted_date = None
                            if bill_record.get('datum'):
                                try:
                                    submitted_date = datetime.strptime(bill_record['datum'], '%Y-%m-%d').date()
                                except ValueError:
                                    pass
                            
                            # Check if bill exists
                            existing = await conn.fetchval(
                                "SELECT id FROM bills WHERE id = $1", bill_id
                            )
                            
                            if existing:
                                # Update bill
                                await conn.execute("""
                                    UPDATE bills 
                                    SET bill_number = $1, title = $2, description = $3,
                                        own_number = $4, bill_type = $5, status = $6,
                                        submitted_date = $7, collection_number = $8,
                                        collection_year = $9, url = $10, updated_at = $11
                                    WHERE id = $12
                                """, bill_record.get('tisk'), bill_record.get('nazev'),
                                    bill_record.get('popis'), bill_record.get('cislo_vlastni'),
                                    bill_record.get('typ'), bill_record.get('stav'),
                                    submitted_date, bill_record.get('cislo_sbirky'),
                                    int(bill_record.get('rok_sbirky')) if bill_record.get('rok_sbirky') else None,
                                    bill_record.get('url'), datetime.now(), bill_id)
                                result.records_updated += 1
                            else:
                                # Insert bill
                                committee_id = int(bill_record.get('id_organ', 165))  # Default to main parliament
                                
                                await conn.execute("""
                                    INSERT INTO bills 
                                    (id, committee_id, bill_number, title, description, own_number,
                                     bill_type, status, submitted_date, collection_number,
                                     collection_year, url)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                """, bill_id, committee_id, bill_record.get('tisk'),
                                    bill_record.get('nazev'), bill_record.get('popis'),
                                    bill_record.get('cislo_vlastni'), bill_record.get('typ'),
                                    bill_record.get('stav'), submitted_date,
                                    bill_record.get('cislo_sbirky'),
                                    int(bill_record.get('rok_sbirky')) if bill_record.get('rok_sbirky') else None,
                                    bill_record.get('url'))
                                result.records_inserted += 1
                        
                        except Exception as e:
                            logger.error(f"Error processing bill {bill_record}: {e}")
                            result.records_failed += 1
            
            result.status = SyncStatus.COMPLETED
            
        except Exception as e:
            result.status = SyncStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Error syncing bills data: {e}")
        
        await self.log_sync_complete(sync_id, result)
        return result
    
    async def run_full_sync(self) -> Dict[str, SyncResult]:
        """Run complete data synchronization"""
        logger.info("Starting full data synchronization...")
        
        results = {}
        
        try:
            # Sync reference data first
            results['electoral_periods'] = await self.sync_electoral_periods()
            results['parties'] = await self.sync_parties()
            results['constituencies'] = await self.sync_constituencies()
            
            # Sync main data
            results['persons_mps'] = await self.sync_persons_and_mps()
            results['voting_data'] = await self.sync_voting_data("hl-2021ps")
            results['bills'] = await self.sync_bills_data()
            
            # Calculate summary
            total_processed = sum(r.records_processed for r in results.values())
            total_inserted = sum(r.records_inserted for r in results.values())
            total_updated = sum(r.records_updated for r in results.values())
            total_failed = sum(r.records_failed for r in results.values())
            
            logger.info(f"Full sync completed: {total_processed} processed, "
                       f"{total_inserted} inserted, {total_updated} updated, "
                       f"{total_failed} failed")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in full sync: {e}")
            raise
    
    async def run_test_sync(self, data_source: str, table_filters: Optional[List[str]] = None) -> SyncResult:
        """Run a test sync on specific data source for troubleshooting
        
        Args:
            data_source: Type of data to sync ('mp', 'voting', 'bills')
            table_filters: Optional list of specific tables to sync
        
        Returns:
            SyncResult for the test operation
        """
        logger.info(f"Running test sync for {data_source}...")
        
        if data_source == 'mp':
            return await self.sync_persons_and_mps(table_filters)
        elif data_source == 'voting':
            return await self.sync_voting_data("hl-2021ps", table_filters)
        elif data_source == 'bills':
            return await self.sync_bills_data(table_filters)
        else:
            raise ValueError(f"Unknown data source: {data_source}")
    
    async def inspect_data_source(self, zip_filename: str) -> Dict[str, Any]:
        """Inspect a data source without processing it
        
        Args:
            zip_filename: Name of the ZIP file to inspect
            
        Returns:
            Dictionary with inspection results
        """
        logger.info(f"Inspecting data source: {zip_filename}")
        
        contents = self.fetcher.inspect_zip_contents(zip_filename)
        schemas = self.fetcher.list_available_schemas()
        
        inspection = {
            'zip_filename': zip_filename,
            'files': contents,
            'available_schemas': {},
            'missing_schemas': []
        }
        
        for filename, size in contents.items():
            table_name = filename.replace('.unl', '')
            if table_name in schemas:
                inspection['available_schemas'][table_name] = schemas[table_name]
            else:
                inspection['missing_schemas'].append(table_name)
        
        return inspection
    
    async def run_incremental_sync(self) -> Dict[str, SyncResult]:
        """Run incremental sync (daily updates)"""
        logger.info("Starting incremental data synchronization...")
        
        results = {}
        
        try:
            # For incremental sync, focus on data that changes frequently
            results['persons_mps'] = await self.sync_persons_and_mps()
            results['voting_data'] = await self.sync_voting_data("hl-2021ps")
            results['bills'] = await self.sync_bills_data()
            
            return results
            
        except Exception as e:
            logger.error(f"Error in incremental sync: {e}")
            raise

class ETLScheduler:
    """Scheduler for automated ETL runs"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pipeline = None
        self.running = False
    
    async def initialize(self):
        """Initialize the scheduler"""
        self.pipeline = ETLPipeline(self.database_url)
        await self.pipeline.initialize()
        logger.info("ETL Scheduler initialized")
    
    async def shutdown(self):
        """Shutdown the scheduler"""
        self.running = False
        if self.pipeline:
            await self.pipeline.shutdown()
        logger.info("ETL Scheduler shutdown")
    
    async def run_daily_sync(self):
        """Run daily synchronization"""
        try:
            logger.info("Starting daily sync...")
            results = await self.pipeline.run_incremental_sync()
            
            # Log summary
            total_processed = sum(r.records_processed for r in results.values())
            total_failed = sum(r.records_failed for r in results.values())
            
            if total_failed > 0:
                logger.warning(f"Daily sync completed with {total_failed} failures out of {total_processed} records")
            else:
                logger.info(f"Daily sync completed successfully: {total_processed} records processed")
            
            return results
            
        except Exception as e:
            logger.error(f"Daily sync failed: {e}")
            raise
    
    async def run_weekly_full_sync(self):
        """Run full synchronization weekly"""
        try:
            logger.info("Starting weekly full sync...")
            results = await self.pipeline.run_full_sync()
            
            logger.info("Weekly full sync completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Weekly full sync failed: {e}")
            raise
    
    async def start_scheduler(self):
        """Start the automated scheduler"""
        self.running = True
        
        logger.info("ETL Scheduler started")
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Run daily sync at 6 AM
                if current_time.hour == 6 and current_time.minute == 0:
                    await self.run_daily_sync()
                
                # Run weekly full sync on Sundays at 2 AM
                elif current_time.weekday() == 6 and current_time.hour == 2 and current_time.minute == 0:
                    await self.run_weekly_full_sync()
                
                # Sleep for 1 minute
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

class ETLMonitor:
    """Monitor and health check for ETL pipeline"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.db = DatabaseManager(database_url)
    
    async def initialize(self):
        """Initialize the monitor"""
        await self.db.initialize()
    
    async def shutdown(self):
        """Shutdown the monitor"""
        await self.db.close()
    
    async def get_last_sync_status(self, sync_type: str = None) -> List[Dict]:
        """Get status of recent sync operations"""
        async with self.db.get_connection() as conn:
            if sync_type:
                records = await conn.fetch("""
                    SELECT * FROM data_sync_log 
                    WHERE sync_type = $1 
                    ORDER BY started_at DESC 
                    LIMIT 10
                """, sync_type)
            else:
                records = await conn.fetch("""
                    SELECT * FROM data_sync_log 
                    ORDER BY started_at DESC 
                    LIMIT 20
                """)
            
            return [dict(record) for record in records]
    
    async def get_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness and quality"""
        async with self.db.get_connection() as conn:
            stats = {}
            
            # Check latest voting session
            latest_vote = await conn.fetchrow("""
                SELECT MAX(vote_date) as latest_date, COUNT(*) as total_sessions
                FROM voting_sessions
                WHERE vote_date IS NOT NULL
            """)
            stats['latest_vote_date'] = latest_vote['latest_date']
            stats['total_voting_sessions'] = latest_vote['total_sessions']
            
            # Check MP count
            mp_count = await conn.fetchval("""
                SELECT COUNT(*) FROM mps WHERE is_active = true
            """)
            stats['active_mps'] = mp_count
            
            # Check recent sync operations
            recent_syncs = await conn.fetch("""
                SELECT sync_type, MAX(completed_at) as last_sync, 
                       COUNT(CASE WHEN sync_status = 'failed' THEN 1 END) as failed_count
                FROM data_sync_log 
                WHERE completed_at > NOW() - INTERVAL '7 days'
                GROUP BY sync_type
                ORDER BY last_sync DESC
            """)
            stats['recent_syncs'] = [dict(sync) for sync in recent_syncs]
            
            # Check for stale data (no updates in 48 hours)
            stale_threshold = datetime.now() - timedelta(hours=48)
            stale_syncs = await conn.fetch("""
                SELECT DISTINCT sync_type 
                FROM data_sync_log 
                WHERE completed_at < $1 AND sync_status = 'completed'
                   OR (completed_at IS NULL AND started_at < $1)
            """, stale_threshold)
            stats['stale_data_types'] = [sync['sync_type'] for sync in stale_syncs]
            
            return stats
    
    async def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive health check"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(),
            'issues': []
        }
        
        try:
            # Check database connectivity
            async with self.db.get_connection() as conn:
                await conn.fetchval("SELECT 1")
            
            # Check data freshness
            freshness = await self.get_data_freshness()
            
            # Check for issues
            if freshness.get('active_mps', 0) < 180:  # Expected ~200 MPs
                health['issues'].append("Low MP count detected")
                health['status'] = 'warning'
            
            if len(freshness.get('stale_data_types', [])) > 0:
                health['issues'].append(f"Stale data detected: {freshness['stale_data_types']}")
                health['status'] = 'warning'
            
            # Check if latest vote is too old
            latest_vote = freshness.get('latest_vote_date')
            if latest_vote and (datetime.now().date() - latest_vote).days > 30:
                health['issues'].append("No recent voting sessions")
                health['status'] = 'warning'
            
            health['data_stats'] = freshness
            
        except Exception as e:
            health['status'] = 'unhealthy'
            health['issues'].append(f"Health check failed: {str(e)}")
        
        return health

# CLI interface for manual operations
async def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Czech Parliament ETL Pipeline")
    parser.add_argument("--database-url", required=True, help="PostgreSQL database URL")
    parser.add_argument("--action", choices=['full-sync', 'incremental-sync', 'monitor', 'scheduler', 'test-sync', 'inspect'], 
                       required=True, help="Action to perform")
    parser.add_argument("--sync-type", help="Specific sync type for monitoring")
    parser.add_argument("--data-source", choices=['mp', 'voting', 'bills'], help="Data source for test-sync")
    parser.add_argument("--tables", nargs='*', help="Specific tables to sync (e.g., osoby hl_hlasovani)")
    parser.add_argument("--zip-file", help="ZIP file to inspect (e.g., poslanci.zip)")
    
    args = parser.parse_args()
    
    if args.action == 'full-sync':
        pipeline = ETLPipeline(args.database_url)
        try:
            await pipeline.initialize()
            results = await pipeline.run_full_sync()
            
            print("\n=== Full Sync Results ===")
            for sync_type, result in results.items():
                print(f"{sync_type}: {result.status.value}")
                print(f"  Processed: {result.records_processed}")
                print(f"  Inserted: {result.records_inserted}")
                print(f"  Updated: {result.records_updated}")
                print(f"  Failed: {result.records_failed}")
                if result.error_message:
                    print(f"  Error: {result.error_message}")
                print()
        finally:
            await pipeline.shutdown()
    
    elif args.action == 'incremental-sync':
        pipeline = ETLPipeline(args.database_url)
        try:
            await pipeline.initialize()
            results = await pipeline.run_incremental_sync()
            
            print("\n=== Incremental Sync Results ===")
            for sync_type, result in results.items():
                print(f"{sync_type}: {result.status.value}")
                print(f"  Records processed: {result.records_processed}")
                if result.error_message:
                    print(f"  Error: {result.error_message}")
        finally:
            await pipeline.shutdown()
    
    elif args.action == 'monitor':
        monitor = ETLMonitor(args.database_url)
        try:
            await monitor.initialize()
            
            # Health check
            health = await monitor.run_health_check()
            print(f"\n=== System Health: {health['status'].upper()} ===")
            if health['issues']:
                print("Issues:")
                for issue in health['issues']:
                    print(f"  - {issue}")
            
            # Recent sync status
            syncs = await monitor.get_last_sync_status(args.sync_type)
            print(f"\n=== Recent Sync Operations ===")
            for sync in syncs[:5]:  # Show last 5
                print(f"{sync['sync_type']} - {sync['sync_status']} - {sync['started_at']}")
                if sync.get('error_message'):
                    print(f"  Error: {sync['error_message']}")
        
        finally:
            await monitor.shutdown()
    
    elif args.action == 'scheduler':
        scheduler = ETLScheduler(args.database_url)
        try:
            await scheduler.initialize()
            print("Starting ETL scheduler... (Press Ctrl+C to stop)")
            await scheduler.start_scheduler()
        except KeyboardInterrupt:
            print("\nShutting down scheduler...")
        finally:
            await scheduler.shutdown()
    
    elif args.action == 'test-sync':
        if not args.data_source:
            print("Please specify --data-source (mp, voting, or bills)")
            return
        
        pipeline = ETLPipeline(args.database_url)
        try:
            await pipeline.initialize()
            
            print(f"Running test sync for {args.data_source}...")
            if args.tables:
                print(f"Filtering tables: {args.tables}")
            
            result = await pipeline.run_test_sync(args.data_source, args.tables)
            
            print(f"\n=== Test Sync Results ===")
            print(f"Status: {result.status.value}")
            print(f"Processed: {result.records_processed}")
            print(f"Inserted: {result.records_inserted}")
            print(f"Updated: {result.records_updated}")
            print(f"Failed: {result.records_failed}")
            if result.error_message:
                print(f"Error: {result.error_message}")
        
        finally:
            await pipeline.shutdown()
    
    elif args.action == 'inspect':
        if not args.zip_file:
            print("Available ZIP files to inspect:")
            print("  --zip-file poslanci.zip   # MP and person data")
            print("  --zip-file tisky.zip      # Bills and documents")
            print("  --zip-file hl-2021ps.zip # Voting data for 2021 period")
            print("  --zip-file organy.zip     # Committees and organs")
            return
        
        pipeline = ETLPipeline(args.database_url)
        try:
            await pipeline.initialize()
            
            inspection = await pipeline.inspect_data_source(args.zip_file)
            
            print(f"\n=== Inspection Results for {args.zip_file} ===")
            print(f"Files found: {len(inspection['files'])}")
            
            for filename, size in inspection['files'].items():
                print(f"  {filename}: {size:,} characters")
            
            print(f"\nTables with schemas: {len(inspection['available_schemas'])}")
            for table, schema in inspection['available_schemas'].items():
                print(f"  {table}: {len(schema)} fields")
            
            if inspection['missing_schemas']:
                print(f"\nTables without schemas: {inspection['missing_schemas']}")
        
        finally:
            await pipeline.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
