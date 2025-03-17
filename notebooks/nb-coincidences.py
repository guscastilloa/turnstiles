# %%
import pandas as pd
import numpy as np
import os
import sys
from pathlib import Path

notebook_dir = Path().absolute()
project_dir = notebook_dir.parent
sys.path.append(str(project_dir))

from src.data.coincidences import coincidenceProcessor
from src.path_setup import get_project_root
# %%
# Define coincidence processor 
processor = coincidenceProcessor()
for i in [2, 3,4,5,6,7]:
    processor.process_single_file(
        file_path= Path(os.getcwd()).parent / "data/intermediate/daily/P2000_20170629.csv",
        window=i
)

# %%
# I'm having issues importing the data. The paths are not being resolved correctly.
# The following dir structure is assumed:
# turnstiles/
# ├── src/
# │   └── data/
# │       └── coincidences.py
# └── notebooks/
#     └── nb-coincidences.py


# %%
# Revise that coincidences processed correcly. The main issue previously was that pairs of even and odd numbers (i.e. 2 and 3) were throwing the exact number of coincidences

p = Path('data' )/ 'intermediate' / 'coincidences'
for i in [2, 3,4,5,6,7]:
    df = pd.read_csv(Path(p / f'coincidences_20170629_window{i}s.csv') )
    print(f"Window: {i}s - {len(df)} coincidences")

# Success!! it's working 

# %%
