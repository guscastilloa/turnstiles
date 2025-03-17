# src/path_setup.py
import os
from pathlib import Path

def get_project_root():
    """Get project root from environment variable, with fallback detection"""
    # First try environment variable
    root = os.environ.get('TORNIQUETES_ROOT')
    
    if root:
        return Path(root)
        
    # Fallback: try to detect project root
    current_path = Path.cwd()
    markers = ['.git', 'pyproject.toml', 'setup.py', 'README.md']
    
    while current_path != current_path.parent:
        if any((current_path / marker).exists() for marker in markers):
            # Found a marker, set the environment variable for future use
            os.environ['TORNIQUETES_ROOT'] = str(current_path)
            return current_path
        current_path = current_path.parent
    
    # Last resort - use current directory and warn
    print("Warning: Could not detect project root. Using current directory.")
    return Path.cwd()

# Create a project paths object
class ProjectPaths:
    def __init__(self):
        self.root = get_project_root()
        
        # Pre-define common paths
        self.data = self.root / 'data'
        self.raw = self.data / 'raw'
        self.processed = self.data / 'processed'
        self.interim = self.data / 'interim'
        self.ground_truth = self.data / 'ground_truth'
        self.src = self.root / 'src'
        self.results = self.root / 'results'
    
    def __call__(self, *path_parts):
        """Resolve path relative to project root"""
        return self.root.joinpath(*path_parts)

# Create singleton instance
paths = ProjectPaths()