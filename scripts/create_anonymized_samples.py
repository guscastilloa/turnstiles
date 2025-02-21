# %%
import os
import sys
import pandas as pd
from pathlib import Path
project_root = (os.path.dirname(os.path.abspath('.')))

# Add the project root to sys.path if it's not already there
if project_root not in sys.path:
    sys.path.append(project_root)
    
from src.data.id_mapper import IDMapper

# %%
# Définir les chemins
INPUT_PATH = Path("data/intermediate/daily/") #P2000_20170629.csv
OUTPUT_PATH = Path("tests/data/")
salt_path = Path("config/secure/salt.key")

# %%
saltpath = Path().cwd().parent / 'config' / 'secure' / 'salt.key'

mapper = IDMapper(
    salt_path= saltpath)
[mapper.create_anonymous_id(original_id="123123") for i in range(3)]

# %%
def create_anonymized_samples(input_dir: Path, output_dir: Path, salt_path: Path):
    """
    Crée des échantillons anonymisés pour les trois sources de données
    
    Args:
        input_dir: Chemin vers le répertoire contenant les données brutes
        output_dir: Chemin vers le répertoire où sauvegarder les résultats
        salt_path: Chemin vers le fichier de sel pour l'anonymisation
    """
    # Initialiser le mapper avec le fichier de sel spécifié
    mapper = IDMapper(salt_path=salt_path)
    
    # 1. Données Tourniquets
    def anonymize_turnstile_sample():
        df_turnstile = pd.read_csv(input_dir / "turnstile/P2000_20160223_sample.csv")
        df_turnstile['carnet'] = df_turnstile['carnet'].astype(str).apply(
            lambda x: mapper.add_identifier(x, source='turnstile')
        )
        return df_turnstile
    
    # 2. Données Trust
    def anonymize_trust_sample():
        df_trust = pd.read_csv(input_dir / "trust/Friends.csv")
        for col in ['participantID', 'friendID']:
            df_trust[col] = df_trust[col].astype(str).apply(
                lambda x: mapper.add_identifier(x, source='trust')
            )
        return df_trust
    
    # 3. Données Survey
    def anonymize_survey_sample():
        df_survey = pd.read_csv(input_dir / "survey/Ciencia_Politica_Amistad_20182.csv")
        first_col = df_survey.columns[0]
        student_ids = set([*df_survey[first_col], *df_survey.columns[1:]])
        
        id_mapping = {
            str(old_id): mapper.add_identifier(str(old_id), source='survey')
            for old_id in student_ids
        }
        
        df_survey_anon = df_survey.copy()
        df_survey_anon.columns = [first_col] + [
            id_mapping[str(col)] for col in df_survey.columns[1:]
        ]
        df_survey_anon[first_col] = df_survey_anon[first_col].map(
            lambda x: id_mapping[str(x)]
        )
        return df_survey_anon

    # Créer les répertoires de sortie
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Anonymiser et sauvegarder les échantillons
    anonymize_turnstile_sample().to_csv(
        output_dir / "turnstile_sample_anon.csv", index=False
    )
    anonymize_trust_sample().to_csv(
        output_dir / "trust_sample_anon.csv", index=False
    )
    anonymize_survey_sample().to_csv(
        output_dir / "survey_sample_anon.csv", index=False
    )