# scripts/local/create_id_mappings.py
from pathlib import Path
import sys
import pandas as pd
import glob
import logging
from typing import List, Dict

root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

from src.data.id_mapper import IDMapper

def setup_logging():
    """Configure script logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger('id_mapper')

def process_turnstile_data(mapper: IDMapper, input_dir: Path, output_dir: Path):
    """Treat all turnstile data files"""
    first_file_written = False
    
    for csv_file in glob.glob(str(input_dir / "P2000*.csv")):
        file_path = Path(csv_file)
        logging.info(f"Processing turnstile file: {file_path.name}")
        
        try:
            try:
                df = pd.read_csv(file_path, delimiter=',')
            except:
                df = pd.read_csv(file_path, delimiter=';')
            df['carnet'] = df['carnet'].astype(str).apply(
                lambda x: mapper.add_identifier(x, source='turnstile')
            )
            
            # Only write first file
            if not first_file_written:
                output_file = output_dir / f"anon_{file_path.name}"
                df.to_csv(output_file, index=False)
                first_file_written = True
                logging.info(f"Written first anonymized file to {output_file}")
        except Exception as e:
            logging.error(f"Error processing {file_path.name}: {str(e)}")

def process_survey_data(mapper: IDMapper, input_dir: Path, output_dir: Path):
    """Treat all MjAlvarez survey data files"""
    survey_patterns = ['*Amistad*.csv', '*Trabajos*.csv', '*Casa*.csv']
    
    for pattern in survey_patterns:
        for csv_file in glob.glob(str(input_dir / "survey" / pattern)):
            file_path = Path(csv_file)
            logging.info(f"Processing survey file: {file_path.name}")
            
            try:
                df = pd.read_csv(file_path)
                # Les ID sont dans la première colonne et les en-têtes
                student_ids = set([df.iloc[:, 0].values[0]] + list(df.columns[1:]))
                for student_id in student_ids:
                    mapper.add_identifier(str(student_id), source='survey')
            except Exception as e:
                logging.error(f"Error processing {file_path.name}: {str(e)}")

def process_trust_data(mapper: IDMapper, input_dir: Path, output_dir: Path):
    """Treat all TrustExperiment data files"""
    trust_files = ['MasterIDsFile.csv', 'Lunch.csv', 'Friends.csv', 
                   'Confide.csv', 'Study.csv', 'MetBefore.csv', 'Saludo.csv']
    
    for filename in trust_files:
        file_path = input_dir / "trust" / filename
        if file_path.exists():
            logging.info(f"Processing trust file: {filename}")
            try:
                # Détecter l'encodage approprié
                encodings = {'MasterIDsFile.csv': 'latin-1'}
                encoding = encodings.get(filename, 'utf-8')
                
                df = pd.read_csv(file_path, encoding=encoding)
                id_column = 'studentID' if filename == 'MasterIDsFile.csv' else 'participantID'
                
                if id_column in df.columns:
                    for student_id in df[id_column].unique():
                        mapper.add_identifier(str(student_id), source='trust')
            except Exception as e:
                logging.error(f"Error processing {filename}: {str(e)}")

def main():
    """Main function to coordinate anonymization process"""
    logger = setup_logging()
    
    # Configuration des chemins
    root_dir = Path(__file__).parent.parent
    input_dir = root_dir / "data/raw"
    output_dir = root_dir / "data/processed"
    salt_path = root_dir / "config/secure/salt.key"
    
    # Créer les répertoires nécessaires
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialiser le mapper
    mapper = IDMapper(salt_path)
    
    # Traiter chaque source de données
    process_turnstile_data(mapper, 
                           input_dir.parent / "intermediate", 
                           root_dir / "tests/data")
    process_survey_data(mapper, input_dir, output_dir / "survey")
    process_trust_data(mapper, input_dir, output_dir / "trust")
    
    # Sauvegarder tous les mappings
    mapper.save_mappings(output_dir / "id_mappings")
    logger.info("Completed ID mapping creation for all sources")

if __name__ == "__main__":
    main()