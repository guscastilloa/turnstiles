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
# Define paths
root_dir = Path(__file__).parent.parent
INPUT_PATH = root_dir / "data" #P2000_20170629.csv
OUTPUT_PATH = root_dir / "tests/data/"
SALT_PATH = root_dir / "config/secure/salt.key"

# %%
saltpath = Path().cwd().parent / 'config' / 'secure' / 'salt.key'

mapper = IDMapper(
    salt_path= saltpath)
[mapper.create_anonymous_id(original_id="123123") for i in range(3)]
# %%

#===============================#
# Convert fecha_completa to datetime


print(len(df.fecha_completa))




# %%
def create_anonymized_samples(input_dir: Path, output_dir: Path, salt_path: Path):
    """
    Create anonymized sample datasets for the three data sources
    """
    # Initialiser le mapper avec le fichier de sel spécifié
    mapper = IDMapper(salt_path=salt_path)
    
    # 1. Turnstile Data
    def anonymize_turnstile_sample():
        df_turnstile = pd.read_csv(input_dir / "intermediate/daily/P2000_20170629.csv", delimiter=';')
        df_turnstile['fecha_completa'] = pd.to_datetime(df_turnstile['fecha_completa'], format='%Y.%m.%d %H:%M:%S')
        # Filter registers after 8 am and before midday
        df_turnstile = df_turnstile[(df_turnstile['fecha_completa'].dt.hour >= 8) & (df_turnstile['fecha_completa'].dt.hour < 10)]
        df_turnstile['carnet'] = df_turnstile['carnet'].astype(str).apply(
            lambda x: mapper.create_anonymous_id(original_id=x)
        )
        return df_turnstile
    
    # 2. Trust Experiment
    def anonymize_trust_sample():
        df_trust = pd.read_csv(input_dir / "raw/trust/Friends.csv")
        for col in ['participantID', 'friendID']:
            df_trust[col] = df_trust[col].astype(str).apply(
                lambda x: mapper.add_identifier(x, source='trust')
            )
        return df_trust
    
    # 3. Survey Data
    def anonymize_survey_sample():
        df_survey = pd.read_csv(input_dir / "raw/survey/Ciencia_Politica_Amistad_20182.csv")
        first_col = df_survey.columns[0]
        student_ids = set([*df_survey[first_col], *df_survey.columns[1:]])
        
        id_mapping = {
            str(old_id): mapper.create_anonymous_id(original_id=str(old_id))
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
    anonymize_turnstile_sample().to_parquet(
        output_dir / "turnstile_sample_anon.parquet", index=False
    )
    # anonymize_trust_sample().to_parquet(
    #     output_dir / "trust_sample_anon.parquet", index=False
    # )
    anonymize_survey_sample().to_parquet(
        output_dir / "survey_sample_anon.parquet", index=False
    )


# %%
# Créer les échantillons anonymisés
create_anonymized_samples(
    input_dir=INPUT_PATH,
    output_dir=OUTPUT_PATH,
    salt_path=SALT_PATH
)
# %%
