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
                
                if len(fields) != len(schema):
                    logger.warning(f"Line {line_num}: Expected {len(schema)} fields, got {len(fields)}")
                    continue
                
                # Create record dict
                record = {}
                for i, field_name in enumerate(schema):
                    record[field_name] = fields[i]
                
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
            'osoba': [
                'id_osoba', 'pred', 'prijmeni', 'jmeno', 'za', 'narozeni',
                'pohlavi', 'zmena', 'umrti'
            ],
            'poslanec': [
                'id_poslanec', 'id_osoba', 'id_kraj', 'id_kandidatka', 'id_organ',
                'web', 'ulice', 'obec', 'psc', 'email', 'telefon', 'fax',
                'psp_telefon', 'facebook', 'foto'
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
            
            # Sessions
            'schuze': [
                'id_organ', 'schuze', 'od_schuze', 'do_schuze', 'aktualizace'
            ],
            
            # Committees
            'organ': [
                'id_organ', 'organ', 'nazev_organ_cz', 'nazev_organ_en',
                'od_organ', 'do_organ', 'cl_organ_base', 'id_typ_organu',
                'zkratka', 'priorita'
            ]
        }
    
    def fetch_mp_data(self) -> List[Dict[str, Any]]:
        """Fetch current MP data (agenda poslanců a osob)"""
        logger.info("Fetching MP data...")
        
        zip_content = self.parser.fetch_data_file("osoby.zip")  # Current data
        files = self.parser.extract_zip_content(zip_content)
        
        all_data = {}
        
        # Parse each file type
        for filename, content in files.items():
            table_name = filename.replace('.unl', '')
            
            if table_name in self.schemas:
                schema = self.schemas[table_name]
                records = self.parser.parse_unl_file(content, schema)
                all_data[table_name] = records
            else:
                logger.warning(f"No schema defined for {table_name}")
        
        return all_data
    
    def fetch_voting_data(self, period: str = "hl-2021ps") -> List[Dict[str, Any]]:
        """Fetch voting data for specific electoral period"""
        logger.info(f"Fetching voting data for period {period}...")
        
        zip_content = self.parser.fetch_data_file(f"{period}.zip")
        files = self.parser.extract_zip_content(zip_content)
        
        all_data = {}
        
        for filename, content in files.items():
            table_name = filename.replace('.unl', '')
            
            if table_name.startswith('hl_'):
                if table_name in self.schemas:
                    schema = self.schemas[table_name]
                    records = self.parser.parse_unl_file(content, schema)
                    all_data[table_name] = records
        
        return all_data
    
    def fetch_bills_data(self) -> List[Dict[str, Any]]:
        """Fetch parliamentary bills data"""
        logger.info("Fetching bills data...")
        
        zip_content = self.parser.fetch_data_file("tisky.zip")
        files = self.parser.extract_zip_content(zip_content)
        
        all_data = {}
        
        for filename, content in files.items():
            table_name = filename.replace('.unl', '')
            
            if table_name in self.schemas:
                schema = self.schemas[table_name]
                records = self.parser.parse_unl_file(content, schema)
                all_data[table_name] = records
        
        return all_data
    
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

# Example usage
def main():
    """Example of how to use the parser"""
    fetcher = ParliamentDataFetcher()
    
    try:
        # Fetch current MP data
        print("Fetching MP data...")
        mp_data = fetcher.fetch_mp_data()
        
        print(f"Found {len(mp_data.get('osoba', []))} persons")
        print(f"Found {len(mp_data.get('poslanec', []))} current MPs")
        
        # Fetch recent voting data
        print("\nFetching voting data...")
        voting_data = fetcher.fetch_voting_data("hl-2021ps")
        
        print(f"Found {len(voting_data.get('hl_hlasovani', []))} voting sessions")
        print(f"Found {len(voting_data.get('hl_poslanec', []))} individual votes")
        
        # Calculate statistics
        print("\nCalculating statistics...")
        stats = fetcher.calculate_mp_stats(voting_data, mp_data)
        
        # Show top 5 MPs by attendance
        sorted_mps = sorted(stats.items(), key=lambda x: x[1]['attendance_rate'], reverse=True)
        
        print("\nTop 5 MPs by attendance rate:")
        for mp_id, stat in sorted_mps[:5]:
            print(f"MP {mp_id}: {stat['attendance_rate']:.1f}% attendance "
                  f"({stat['votes_for']} for, {stat['votes_against']} against, "
                  f"{stat['abstentions']} abstentions)")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()