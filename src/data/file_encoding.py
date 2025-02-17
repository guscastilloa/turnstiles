# src/data/file_encoding.py

import os
import logging
from pathlib import Path
from typing import Union, List, Optional, Tuple
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def detect_file_encoding(
    file_path: Union[str, Path], 
    encodings: Optional[List[str]] = None,
    read_as_csv: bool = True
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Detect the correct encoding for a file by attempting to read it with different encodings.
    
    Args:
        file_path: Path to the file to check
        encodings: List of encodings to try. If None, uses default encodings
        read_as_csv: If True, attempts to read as CSV. If False, reads as text
    
    Returns:
        Tuple of (detected encoding, DataFrame if read_as_csv=True else None)
        Returns (None, None) if no working encoding is found
    
    Example:
        >>> encoding, df = detect_file_encoding("data/myfile.csv")
        >>> if encoding:
        >>>     print(f"File can be read with {encoding} encoding")
    """
    if encodings is None:
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'ISO-8859-1']
    
    file_path = Path(file_path)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return None, None
    
    for encoding in encodings:
        try:
            if read_as_csv:
                df = pd.read_csv(file_path, encoding=encoding)
                logger.info(f"Successfully read {file_path.name} with {encoding} encoding")
                return encoding, df
            else:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read()
                logger.info(f"Successfully read {file_path.name} with {encoding} encoding")
                return encoding, None
        except UnicodeDecodeError:
            logger.debug(f"Encoding {encoding} failed for {file_path.name}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error reading {file_path.name} with {encoding}: {str(e)}")
            continue
    
    logger.warning(f"No working encoding found for {file_path.name}")
    return None, None

def batch_detect_encodings(
    directory: Union[str, Path],
    pattern: str = "*.csv",
    encodings: Optional[List[str]] = None
) -> dict:
    """
    Detect encodings for multiple files in a directory.
    
    Args:
        directory: Directory containing files to check
        pattern: Glob pattern for file matching
        encodings: List of encodings to try
    
    Returns:
        Dictionary mapping filenames to their detected encodings
    """
    directory = Path(directory)
    results = {}
    
    for file_path in directory.glob(pattern):
        encoding, _ = detect_file_encoding(file_path, encodings=encodings)
        results[file_path.name] = encoding
    
    return results

# Simple unit tests
def _run_tests():
    """Run basic unit tests for the encoding detection functions."""
    import tempfile
    import pytest
    
    # Test with a known UTF-8 file
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.csv', delete=False) as f:
        f.write('Hello,World\n1,2')
        temp_path = f.name
    
    try:
        # Test single file detection
        encoding, df = detect_file_encoding(temp_path)
        assert encoding == 'utf-8', f"Expected utf-8, got {encoding}"
        assert df is not None, "DataFrame should not be None for CSV"
        assert df.shape == (1, 2), f"Expected shape (1, 2), got {df.shape}"
        
        # Test batch detection
        results = batch_detect_encodings(Path(temp_path).parent, pattern=Path(temp_path).name)
        assert len(results) == 1, f"Expected 1 result, got {len(results)}"
        assert list(results.values())[0] == 'utf-8'
        
        print("All tests passed!")
        
    finally:
        os.unlink(temp_path)

if __name__ == "__main__":
    _run_tests()
