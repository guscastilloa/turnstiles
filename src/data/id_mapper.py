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
    """
    
    def __init__(self, salt_path: Optional[Path] = None):
        self.salt = self._load_salt(salt_path) 
        self.mappings: Dict[str, Dict] = {
            'turnstile': {},
            'trust': {},
            'survey': {}
        }
        self._setup_logging()
    
    def _setup_logging(self):
        self.logger = logging.getLogger('IDMapper')
            
    def _load_salt(self, salt_path: Optional[Path]) -> bytes:
        """
        Load existing salt. If not found, raise an error.
        The salt is used to create a deterministic mapping between original IDs and anonymous IDs. If the original salt isn't used this might cause compatibility issues with previously anonymized IDs.
        """
        if salt_path is None:
            raise ValueError("salt_path must be provided")
            salt_path = Path('config/secure/salt.key')
            
        salt_path.parent.mkdir(parents=True, exist_ok=True)
        
        if salt_path.exists():
            with open(salt_path, 'rb') as f:
                return f.read()
        else:
            raise FileNotFoundError(
                f"Salt file not found. Please create salt file with original salt at {salt_path}."
            )
    
    def create_anonymous_id(self, original_id: str) -> str:
        """Generate deterministic anonymous ID."""
        return hashlib.sha256(
            f"{original_id}{self.salt}".encode()
        ).hexdigest()[:12]
    
    def add_identifier(self, original_id: str, source: str) -> str:
        """Add a new identifier mapping and return its anonymized version.
        This method creates a new mapping for an original identifier from a specific source,
        generating an anonymous ID along with timestamp. If the mapping
        already exists, it returns the existing anonymous ID.

        Args:
            original_id (str): The original identifier to be anonymized
            source (str): The data source category for the identifier
        Returns:
            str: The anonymized identifier
        Raises:
            ValueError: If the specified source is not registered in the mappings
        Example:
            >>> mapper = IdMapper()
            >>> anon_id = mapper.add_identifier("user123", "turnstile")
            >>> print(anon_id)  # Returns something like 'CUST_7B4E2A'
        """
        if source not in self.mappings:
            raise ValueError(f"Invalid source: {source}")
            
        if original_id not in self.mappings[source]:
            timestamp = datetime.now().isoformat()
            anon_id = self.create_anonymous_id(original_id)
            
            self.mappings[source][original_id] = {
                'anonymous_id': anon_id,
                'timestamp': timestamp
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
                        'timestamp': data['timestamp']
                    }
                    for orig_id, data in self.mappings[source].items()
                ])
                
                output_file = output_dir / f"id_mapping_{source}.csv"
                df.to_csv(output_file, index=False)
                self.logger.info(f"Saved {source} mappings to {output_file}")