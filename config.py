# config.py

import os
import platform
from pathlib import Path
from enum import Enum

class Phase(Enum):
    """Project phases enum to avoid string literals"""
    BUILD = "build"
    ANALYSIS = "analysis"

class ProjectConfig:
    """Manages project paths and configuration across different environments and phases"""
    
    def __init__(self, phase=Phase.BUILD):
        # Get the project root directory (where the config.py file is located)
        self.project_root = Path(__file__).parent.absolute()
        self.phase = phase
        
        # Define environment-specific base paths
        self.env_paths = {
            'hypatia': {
                'base': '/hpcfs/home/economia/ga.castillo/projects/TOR/turnstiles',
            },
            'local': {
                'base': str(self.project_root),
            }
        }
        
        # Detect current environment
        self.environment = self._detect_environment()
        
        # Set up phase-specific paths
        self._setup_paths()
        
    def _detect_environment(self):
        """Detect whether we're running on Hypatia or locally"""
        if os.path.exists('/hpcfs/home/economia'):
            return 'hypatia'
        return 'local'
    
    def _setup_paths(self):
        """Set up paths based on the current phase"""
        base_dir = self.env_paths[self.environment]['base']
        phase_num = "01" if self.phase == Phase.BUILD else "02"
        
        self.paths = {
            'input': Path(base_dir) / f"{phase_num}_{self.phase.value}/01_input",
            'scripts': Path(base_dir) / f"{phase_num}_{self.phase.value}/02_scripts",
            'output': Path(base_dir) / f"{phase_num}_{self.phase.value}/03_output",
            'temp': Path(base_dir) / f"{phase_num}_{self.phase.value}/04_temp",
        }
        
        # Add phase-specific paths
        if self.phase == Phase.BUILD:
            self.paths.update({
                'daily': self.paths['output'] / 'Daily',
                'coincidences': self.paths['output'] / 'Coincidences',
                'raw_data': self.paths['input'] / 'P2000'
            })
        elif self.phase == Phase.ANALYSIS:
            self.paths.update({
                'figures': self.paths['output'] / 'Figures',
                'tables': self.paths['output'] / 'Tables',
                'networks': self.paths['output'] / 'Networks',
                'models': self.paths['output'] / 'Models'
            })
    
    def ensure_directories(self):
        """Create all necessary directories if they don't exist"""
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)
    
    def get_path(self, key):
        """Get the appropriate path for the current environment"""
        return str(self.paths[key])
    
    def __str__(self):
        """Pretty print current configuration"""
        return f"Environment: {self.environment}\n" + \
               f"Phase: {self.phase.value}\n" + \
               "Paths:\n" + \
               "\n".join(f"  {k}: {v}" for k, v in self.paths.items())