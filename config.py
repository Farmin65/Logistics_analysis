
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REPORTS_DIR = BASE_DIR / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"

for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR, FIGURES_DIR, NOTEBOOKS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

class Config:
    # Параметры для выявления аномалий
    ANOMALY_THRESHOLD_STD = 2.5  
    ANOMALY_THRESHOLD_IQR = 1.5  
    
    # Параметры маршрутов
    MAX_DETOUR_RATIO = 1.5 
    MAX_SPEED_KPH = 120     
    MIN_SPEED_KPH = 10      
    
   
    DB_PATH = BASE_DIR / "logistics.db"
    DB_URL = f"sqlite:///{DB_PATH}"
