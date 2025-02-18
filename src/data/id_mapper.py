# Path: src/data/anonymization/id_mapper.py

import os
import pandas as pd
import hashlib
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, Dict, List

class IDMapper:
    """
    Manages anonymous ID mapping for turnstile research project.
    
    The hash verification column is a checksum that helps verify data integrity
    and detect any tampering with the mapping files. It's created by hashing
    the combination of original ID, anonymous ID and timestamp.
    """
    
    def __init__(self, salt_path: Optional[Path] = None):
        self.salt = self._load_or_create_salt(salt_path)
        self.mappings: Dict[str, Dict] = {
            'turnstile': {},
            'trust': {},
            'survey': {}
        }
        self._setup_logging()
    
    def _setup_logging(self):
        self.logger = logging.getLogger('IDMapper')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        
        # File handler
        fh = logging.FileHandler('id_mapping.log')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
    
    def _load_or_create_salt(self, salt_path: Optional[Path]) -> bytes:
        """Load existing salt or create new one if not exists."""
        if salt_path is None:
            salt_path = Path('config/secure/salt.key')
            
        salt_path.parent.mkdir(parents=True, exist_ok=True)
        
        if salt_path.exists():
            with open(salt_path, 'rb') as f:
                return f.read()
        else:
            salt = os.urandom(32)
            with open(salt_path, 'wb') as f:
                f.write(salt)
            return salt
    
    def create_anonymous_id(self, original_id: str) -> str:
        """Generate deterministic anonymous ID."""
        return hashlib.sha256(
            f"{original_id}{self.salt}".encode()
        ).hexdigest()[:12]
    
    def create_verification_hash(self, original_id: str, 
                               anon_id: str, 
                               timestamp: str) -> str:
        """Create verification hash to detect tampering."""
        return hashlib.sha256(
            f"{original_id}{anon_id}{timestamp}{self.salt}".encode()
        ).hexdigest()
    
    def add_identifier(self, original_id: str, source: str) -> str:
        """Add new identifier and return its anonymous ID."""
        if source not in self.mappings:
            raise ValueError(f"Invalid source: {source}")
            
        if original_id not in self.mappings[source]:
            timestamp = datetime.now().isoformat()
            anon_id = self.create_anonymous_id(original_id)
            verification = self.create_verification_hash(original_id, anon_id, timestamp)
            
            self.mappings[source][original_id] = {
                'anonymous_id': anon_id,
                'timestamp': timestamp,
                'verification': verification
            }
            self.logger.info(f"Added new {source} mapping: {original_id} -> {anon_id}")
            
        return self.mappings[source][original_id]['anonymous_id']
    
    def save_mappings(self, output_dir: Path):
        """Save mapping tables for each source."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for source in self.mappings:
            if self.mappings[source]:
                df = pd.DataFrame([
                    {
                        'original_id': orig_id,
                        'anonymous_id': data['anonymous_id'],
                        'timestamp': data['timestamp'],
                        'verification': data['verification']
                    }
                    for orig_id, data in self.mappings[source].items()
                ])
                
                output_file = output_dir / f"id_mapping_{source}.csv"
                df.to_csv(output_file, index=False)
                self.logger.info(f"Saved {source} mappings to {output_file}")