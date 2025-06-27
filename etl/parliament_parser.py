import requests
import zipfile
import io
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from dataclasses import dataclass
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ParsedRecord:
    """Represents a parsed record from UNL format"""
    data: Dict[str, Any]
    source_file: str
    parsed_at: datetime

class UNLParser:
    """Parser for Czech Parliament UNL format data"""
    
    def __init__(self):
        self.base_url = "https://www.psp.cz/eknih/cdrom/opendata"
        self.encoding = "windows-1250"
        self.delimiter = "|"  # ASCII 124
        self.escape_char = "\\"  # ASCII 092
        
    def fetch_data_file(self, filename: str) -> bytes:
        """Download and return ZIP file content"""
        url = f"{self.base_url}/{filename}"
        logger.info(f"Fetching {url}")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {filename}: {e}")
            raise
    
    def extract_zip_content(self, zip_content: bytes) -> Dict[str, str]:
        """Extract all files from ZIP and return as dict"""
        files = {}
        
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_info in zip_file.filelist:
                if not file_info.is_dir():
                    try:
                        content = zip_file.read(file_info.filename)
                        # Decode using windows-1250
                        text_content = content.decode(self.encoding)
                        files[file_info.filename] = text_content
                        logger.info(f"Extracted {file_info.filename} ({len(text_content)} chars)")
                    except UnicodeDecodeError as e:
                        logger.warning(f"Failed to decode {file_info.filename}: {e}")
                        
        return files
    
    def parse_unl_line(self, line: str) -> List[Optional[str]]:
        """Parse a single UNL line handling escape sequences"""
        if not line.strip():
            return []
            
        # Handle escape sequences
        line = line.replace("\\|", "\x00")  # Temporarily replace escaped delimiters
        line = line.replace("\\\\", "\x01")  # Temporarily replace escaped backslashes
        
        # Split by delimiter
        fields = line.split(self.delimiter)
        
        # Restore escaped characters and handle null values
        processed_fields = []
        for field in fields:
            field = field.replace("\x00", "|")  # Restore escaped delimiters
            field = field.replace("\x01", "\\")  # Restore escaped backslashes
            
            # Handle null values
            if field.strip() == "":
                processed_fields.append(None)
            else:
                processed_fields.append(field.strip())
                
        return processed_fields
    
    def parse_unl_file(self, content: str, schema: List[str]) -> List[Dict[str, Any]]:
        """Parse entire UNL file using provided schema"""
        records = []
        lines = content.strip().split('\n')
        
        logger.info(f"Parsing {len(lines)} lines with schema: {schema}")
        
        for line_num, line in enumerate(lines, 1):
            try:
                fields = self.parse_unl_line(line)
                
                # Handle field count mismatches
                if len(fields) < len(schema):
                    logger.warning(f"Line {line_num}: Expected {len(schema)} fields, got {len(fields)} - skipping line")
                    continue
                elif len(fields) > len(schema):
                    logger.debug(f"Line {line_num}: Got {len(fields)} fields, expected {len(schema)} - ignoring extra fields")
                
                # Create record dict using only the fields defined in schema
                record = {}
                for i, field_name in enumerate(schema):
                    if i < len(fields):
                        record[field_name] = fields[i]
                    else:
                        record[field_name] = None  # In case of missing fields
                
                records.append(record)
                
            except Exception as e:
                logger.error(f"Error parsing line {line_num}: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(records)} records")
        return records

class ParliamentDataFetcher:
    """Main class for fetching and parsing Czech Parliament data"""
    
    def __init__(self):
        self.parser = UNLParser()
        
        # Define schemas for different data types
        self.schemas = {
            # MP and persons data
            'osoby': [
                'id_osoba', 'pred', 'prijmeni', 'jmeno', 'za', 'narozeni',
                'pohlavi', 'zmena', 'umrti'
            ],
            'poslanec': [
                'id_poslanec', 'id_osoba', 'id_kraj', 'id_kandidatka', 'id_organ',
                'web', 'ulice', 'obec', 'psc', 'email', 'telefon', 'fax',
                'psp_telefon', 'facebook', 'foto'
            ],
            'zarazeni': [
                'id_osoba', 'id_of', 'cl_funkce', 'od_o', 'do_o', 'od_f', 'do_f'
            ],
            'omluvy': [
                'id_poslanec', 'den', 'od', 'do'
            ],
            'pkgps': [
                'id_poslanec', 'adresa', 'sirka', 'delka'
            ],
            'osoba_extra': [
                'id_osoba', 'id_org', 'typ', 'obvod', 'strana', 'id_external'
            ],
            
            # Voting data
            'hl_hlasovani': [
                'id_hlasovani', 'id_organ', 'schuze', 'cislo', 'bod', 'datum',
                'cas', 'pro', 'proti', 'zdrzel', 'nehlasoval', 'prihlaseno',
                'kvorum', 'druh_hlasovani', 'vysledek', 'nazev_dlouhy', 'nazev_kratky'
            ],
            'hl_poslanec': [
                'id_hlasovani', 'id_poslanec', 'vysledek'  # A/N/Z/@/M/X/0
            ],
            
            # Bills and documents
            'tisk': [
                'id_tisk', 'id_organ', 'tisk', 'nazev', 'popis', 'cislo_vlastni',
                'typ', 'stav', 'datum', 'cislo_sbirky', 'rok_sbirky', 'url'
            ],
            'tisk_stav': [
                'id_tisk', 'stav', 'datum'
            ],
            
            # Sessions
            'schuze': [
                'id_organ', 'schuze', 'od_schuze', 'do_schuze', 'aktualizace'
            ],
            'schuze_stav': [
                'id_organ', 'schuze', 'stav', 'typ', 'datum_stavu', 'poznamka'
            ],
            'bod_schuze': [
                'id_organ', 'schuze', 'id_tisk', 'bod', 'uplny_naz', 'poznamka', 'id_typ', 'rj'
            ],
            
            # Committees and organs
            'organy': [
                'id_organ', 'organ_id_organ', 'id_typ_organu', 'zkratka', 
                'nazev_organu_cz', 'nazev_organu_en', 'od_organ', 'do_organ', 
                'priorita', 'cl_organ_base'
            ],
            'typ_organu': [
                'id_typ_org', 'typ_id_typ_org', 'nazev_typ_org_cz', 
                'nazev_typ_org_en', 'typ_org_obecny', 'priorita'
            ],
            'funkce': [
                'id_funkce', 'id_organ', 'id_typ_funkce', 'nazev_funkce_cz', 'priorita'
            ],
            'typ_funkce': [
                'id_typ_funkce', 'id_typ_org', 'typ_funkce_cz', 'typ_funkce_en', 
                'priorita', 'typ_funkce_obecny'
            ],
            
            # Stenographic records
            'stenozaznam': [
                'id_organ', 'schuze', 'den', 'start', 'stop', 'turn'
            ],
            
            # Interpellations  
            'interpelace': [
                'id_interpelace', 'id_organ', 'poradove_cislo', 'schuze',
                'datum', 'typ_interpelace', 'adresat', 'nazev'
            ],
            
            # Committees and working groups
            'tz_poslanec': [
                'id_poslanec', 'id_tz', 'funkce'
            ],
            'tz': [
                'id_tz', 'zkratka', 'nazev', 'id_organ'
            ]
        }
    
    def fetch_data_source(self, zip_filename: str, table_filters: Optional[List[str]] = None) -> Dict[str, Any]:
        """Generic method to fetch and parse any data source
        
        Args:
            zip_filename: Name of the ZIP file to download (e.g., 'poslanci.zip')
            table_filters: Optional list of table names to process (e.g., ['osoby', 'poslanec'])
                          If None, processes all tables found in the ZIP
        
        Returns:
            Dictionary with table names as keys and parsed records as values
        """
        logger.info(f"Fetching data from {zip_filename}...")
        
        zip_content = self.parser.fetch_data_file(zip_filename)
        files = self.parser.extract_zip_content(zip_content)
        
        all_data = {}
        
        # Parse each file type
        for filename, content in files.items():
            table_name = filename.replace('.unl', '')
            
            # Apply table filter if specified
            if table_filters and table_name not in table_filters:
                logger.info(f"Skipping {table_name} (not in filter list)")
                continue
            
            if table_name in self.schemas:
                schema = self.schemas[table_name]
                records = self.parser.parse_unl_file(content, schema)
                all_data[table_name] = records
                logger.info(f"Processed {table_name}: {len(records)} records")
            else:
                logger.warning(f"No schema defined for {table_name}")
                # Still include raw content for debugging
                all_data[f"{table_name}_raw"] = content[:1000] + "..." if len(content) > 1000 else content
        
        return all_data
    
    def fetch_mp_data(self, table_filters: Optional[List[str]] = None) -> Dict[str, Any]:
        """Fetch current MP data (agenda poslanců a osob)
        
        Args:
            table_filters: Optional list of specific tables to fetch (e.g., ['osoby', 'poslanec'])
        """
        return self.fetch_data_source("poslanci.zip", table_filters)
    
    def fetch_voting_data(self, period: str = "hl-2021ps", table_filters: Optional[List[str]] = None) -> Dict[str, Any]:
        """Fetch voting data for specific electoral period
        
        Args:
            period: Electoral period identifier (e.g., 'hl-2021ps')
            table_filters: Optional list of specific tables to fetch (e.g., ['hl_hlasovani', 'hl_poslanec'])
        """
        logger.info(f"Fetching voting data for period {period}...")
        
        zip_content = self.parser.fetch_data_file(f"{period}.zip")
        files = self.parser.extract_zip_content(zip_content)
        
        all_data = {}
        
        for filename, content in files.items():
            table_name = filename.replace('.unl', '')
            
            # Apply table filter if specified
            if table_filters and table_name not in table_filters:
                logger.info(f"Skipping {table_name} (not in filter list)")
                continue
            
            if table_name.startswith('hl_'):
                if table_name in self.schemas:
                    schema = self.schemas[table_name]
                    records = self.parser.parse_unl_file(content, schema)
                    all_data[table_name] = records
                    logger.info(f"Processed {table_name}: {len(records)} records")
                else:
                    logger.warning(f"No schema defined for {table_name}")
                    # Still include raw content for debugging
                    all_data[f"{table_name}_raw"] = content[:1000] + "..." if len(content) > 1000 else content
        
        return all_data
    
    def fetch_bills_data(self, table_filters: Optional[List[str]] = None) -> Dict[str, Any]:
        """Fetch parliamentary bills data
        
        Args:
            table_filters: Optional list of specific tables to fetch (e.g., ['tisk'])
        """
        return self.fetch_data_source("tisky.zip", table_filters)
    
    def calculate_mp_stats(self, voting_data: Dict[str, List], mp_data: Dict[str, List]) -> Dict[str, Dict]:
        """Calculate statistics for each MP"""
        logger.info("Calculating MP statistics...")
        
        mp_stats = {}
        
        # Get voting records
        votes = voting_data.get('hl_poslanec', [])
        sessions = voting_data.get('hl_hlasovani', [])
        
        # Create lookup for session info
        session_lookup = {s['id_hlasovani']: s for s in sessions}
        
        # Calculate stats per MP
        for vote in votes:
            mp_id = vote['id_poslanec']
            vote_result = vote['vysledek']
            
            if mp_id not in mp_stats:
                mp_stats[mp_id] = {
                    'total_votes': 0,
                    'votes_for': 0,
                    'votes_against': 0,
                    'abstentions': 0,
                    'absent': 0,
                    'attendance_rate': 0.0
                }
            
            stats = mp_stats[mp_id]
            stats['total_votes'] += 1
            
            # A=ano/for, N=ne/against, Z=zdržel se/abstain, @=nepřihlášen, M=omluven, X=nehlasoval, 0=nezaznamenán
            if vote_result == 'A':
                stats['votes_for'] += 1
            elif vote_result == 'N':
                stats['votes_against'] += 1
            elif vote_result == 'Z':
                stats['abstentions'] += 1
            else:  # @, M, X, 0
                stats['absent'] += 1
        
        # Calculate attendance rates
        for mp_id, stats in mp_stats.items():
            if stats['total_votes'] > 0:
                present_votes = stats['votes_for'] + stats['votes_against'] + stats['abstentions']
                stats['attendance_rate'] = (present_votes / stats['total_votes']) * 100
        
        return mp_stats
    
    def list_available_schemas(self) -> Dict[str, List[str]]:
        """List all available table schemas for debugging"""
        return self.schemas.copy()
    
    def inspect_zip_contents(self, zip_filename: str) -> Dict[str, int]:
        """Inspect ZIP file contents without parsing - useful for debugging
        
        Args:
            zip_filename: Name of the ZIP file to inspect
            
        Returns:
            Dictionary with filename as key and content length as value
        """
        logger.info(f"Inspecting contents of {zip_filename}...")
        
        zip_content = self.parser.fetch_data_file(zip_filename)
        files = self.parser.extract_zip_content(zip_content)
        
        contents = {}
        for filename, content in files.items():
            contents[filename] = len(content)
            table_name = filename.replace('.unl', '')
            has_schema = table_name in self.schemas
            logger.info(f"  {filename}: {len(content)} chars, schema: {'✓' if has_schema else '✗'}")
        
        return contents

# Example usage and debugging
def main():
    """Example of how to use the parser with cherry-picking capabilities"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Czech Parliament Data Parser")
    parser.add_argument("--action", choices=['inspect', 'schemas', 'fetch', 'demo'], 
                       default='demo', help="Action to perform")
    parser.add_argument("--source", help="Data source ZIP file (e.g., poslanci.zip)")
    parser.add_argument("--tables", nargs='*', help="Specific tables to fetch (e.g., osoby poslanec)")
    parser.add_argument("--period", default="hl-2021ps", help="Electoral period for voting data")
    
    args = parser.parse_args()
    
    fetcher = ParliamentDataFetcher()
    
    try:
        if args.action == 'schemas':
            print("Available table schemas:")
            schemas = fetcher.list_available_schemas()
            for table, fields in schemas.items():
                print(f"  {table}: {', '.join(fields)}")
        
        elif args.action == 'inspect':
            if not args.source:
                print("Available data sources to inspect:")
                sources = ['poslanci.zip', 'tisky.zip', 'hl-2021ps.zip', 'organy.zip']
                for source in sources:
                    print(f"  python parliament_parser.py --action inspect --source {source}")
                return
            
            contents = fetcher.inspect_zip_contents(args.source)
            print(f"\nContents of {args.source}:")
            for filename, size in contents.items():
                print(f"  {filename}: {size:,} characters")
        
        elif args.action == 'fetch':
            if not args.source:
                print("Please specify --source (e.g., poslanci.zip)")
                return
            
            print(f"Fetching data from {args.source}...")
            if args.tables:
                print(f"Filtering tables: {args.tables}")
            
            data = fetcher.fetch_data_source(args.source, args.tables)
            
            print(f"\nFetched data summary:")
            for table, records in data.items():
                if isinstance(records, list):
                    print(f"  {table}: {len(records)} records")
                else:
                    print(f"  {table}: raw content ({len(str(records))} chars)")
        
        elif args.action == 'demo':
            # Original demo functionality
            print("=== DEMO MODE ===")
            
            # Fetch just person data for troubleshooting
            print("1. Fetching only person data...")
            mp_data = fetcher.fetch_mp_data(['osoby'])
            print(f"Found {len(mp_data.get('osoby', []))} persons")
            
            # Fetch just voting sessions (not individual votes)
            print("\n2. Fetching only voting sessions...")
            voting_data = fetcher.fetch_voting_data(args.period, ['hl_hlasovani'])
            print(f"Found {len(voting_data.get('hl_hlasovani', []))} voting sessions")
            
            # Show sample data
            if mp_data.get('osoby'):
                print(f"\nSample person record: {mp_data['osoby'][0]}")
            
            if voting_data.get('hl_hlasovani'):
                print(f"\nSample voting session: {voting_data['hl_hlasovani'][0]}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()