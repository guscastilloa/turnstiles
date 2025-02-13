import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytest

class TurnstileTestGenerator:
    """Generates synthetic turnstile data for testing"""
    
    def __init__(self, date="2016-02-23", n_students=100, n_records=1000):
        self.date = pd.to_datetime(date)
        self.n_students = n_students
        self.n_records = n_records
        
        # Common configurations
        self.programs = [
            "ECONOMIA", "ADMINISTRACION", "INGENIERIA CIVIL",
            "MEDICINA", "DERECHO", "FISICA"
        ]
        self.buildings = ["AU", "ML", "W", "SD", "FRANCO"]
        self.access_types = ["IN", "OUT"]
        
    def _generate_student_ids(self):
        """Generate realistic student IDs (format: YYYYNNNNN)"""
        base_years = np.random.choice(range(2010, 2016), self.n_students)
        return [f"{year}{str(i).zfill(5)}" for i, year in enumerate(base_years)]
    
    def _generate_timestamps(self):
        """Generate timestamps following class schedule patterns"""
        base_times = pd.date_range(
            start=f"{self.date.date()} 07:00",
            end=f"{self.date.date()} 21:00",
            freq='30min'
        )
        
        # Add random minutes
        timestamps = []
        for base_time in base_times:
            n_per_slot = np.random.poisson(self.n_records / len(base_times))
            random_minutes = np.random.randint(0, 30, n_per_slot)
            timestamps.extend([base_time + timedelta(minutes=m) for m in random_minutes])
        
        return sorted(timestamps)[:self.n_records]
    
    def generate_dataset(self):
        """Generate a complete synthetic dataset"""
        student_ids = self._generate_student_ids()
        timestamps = self._generate_timestamps()
        
        data = {
            "PROGRAMA ESTUDIANTE": np.random.choice(self.programs, self.n_records),
            "carnet": np.random.choice(student_ids, self.n_records),
            "porteria_detalle": [f"{np.random.choice(self.buildings)}-MOL{np.random.randint(1,6)}-{np.random.choice(self.access_types)}-T{np.random.randint(1,12)}" for _ in range(self.n_records)],
            "edificio": np.random.choice(self.buildings, self.n_records),
            "porteria": np.random.choice(self.buildings, self.n_records),
            "tipoacceso": np.random.choice(self.access_types, self.n_records),
            "modo_acceso": "Peatonal",
            "fecha_completa": timestamps,
            "n_diasemana": self.date.weekday() + 1,
            "s_dia": self.date.strftime("%a"),
            "semana_anio": self.date.isocalendar()[1],
            "jornada": np.random.choice(["Mañana", "Tarde", "Noche"], self.n_records)
        }
        
        df = pd.DataFrame(data)
        return df.sort_values("fecha_completa").reset_index(drop=True)

gen = TurnstileTestGenerator()
df = gen.generate_dataset()

# # Unit tests
# def test_generator_basics():
#     gen = TurnstileTestGenerator()
#     df = gen.generate_dataset()
    
#     assert len(df) == 1000
#     assert all(col in df.columns for col in [
#         "PROGRAMA ESTUDIANTE", "carnet", "porteria_detalle",
#         "fecha_completa", "tipoacceso"
#     ])
#     assert df.fecha_completa.is_monotonic_increasing

# def test_student_id_format():
#     gen = TurnstileTestGenerator()
#     df = gen.generate_dataset()
    
#     # Check student ID format (YYYYNNNNN)
#     assert all(str(id).isdigit() and len(str(id)) == 9 for id in df.carnet)
#     assert all(2010 <= int(str(id)[:4]) <= 2015 for id in df.carnet)

# if __name__ == "__main__":
#     # Generate example dataset
#     generator = TurnstileTestGenerator()
#     test_df = generator.generate_dataset()
#     print(f"Generated {len(test_df)} records")
#     print("\nSample of generated data:")
#     print(test_df.head())