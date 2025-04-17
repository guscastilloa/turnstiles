# tests/unit/test_anonymizer.py
import unittest
import sys
import os
from pathlib import Path
sys.path.append('.')
from src.data.id_mapper import IDMapper #, create_anonymous_id

# ====================================================================#
# Walk up the directory tree until you find the project root (contains '.git')
current = os.path.abspath(os.path.dirname(__file__))
while '.git' not in os.listdir(current):
    parent = os.path.dirname(current)
    if parent == current:  # Reached root of filesystem
        break
    current = parent

ROOT_DIR = current
# Add the detected project root to sys.path
sys.path.append(ROOT_DIR)
# ====================================================================#

# Define paths
ROOT_DIR = Path(ROOT_DIR)
INPUT_PATH = ROOT_DIR / "data" #P2000_20170629.csv
OUTPUT_PATH = ROOT_DIR / "tests/data/"
SALT_PATH = ROOT_DIR / "config/secure/salt.key"

class TestAnonymizer(unittest.TestCase):
    def setUp(self):
        self.mapper = IDMapper(salt_path = SALT_PATH)
        
    def test_create_anonymous_id(self):
        """Test anonymous ID creation is deterministic"""
        input_value = "123456"
        expected_output = 'b13560b07a60'
        self.assertEqual(self.mapper.create_anonymous_id(input_value), expected_output)

    def test_create_anonymous_id_consistency(self):
        """Ensure the same input produces the same anonymous ID"""
        input_value = "987654"
        self.assertEqual(self.mapper.create_anonymous_id(input_value), self.mapper.create_anonymous_id(input_value))

    def test_create_anonymous_id_different_inputs(self):
        """Ensure different inputs produce different anonymous IDs"""
        input1 = "abc"
        input2 = "def"
        self.assertNotEqual(self.mapper.create_anonymous_id(input1), self.mapper.create_anonymous_id(input2))
 