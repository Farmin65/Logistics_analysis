
"""
Конфигурация проекта для анализа логистики
"""
import os
from pathlib import Path

# Базовые пути
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REPORTS_DIR = BASE_DIR / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"

# Создание директорий
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR, FIGURES_DIR, NOTEBOOKS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Параметры анализа
class Config:
    # Параметры для выявления аномалий
    ANOMALY_THRESHOLD_STD = 2.5  # Количество стандартных отклонений
    ANOMALY_THRESHOLD_IQR = 1.5  # Коэффициент для IQR метода
    
    # Параметры маршрутов
    MAX_DETOUR_RATIO = 1.5  # Максимальное соотношение фактического/оптимального пути
    MAX_SPEED_KPH = 120     # Максимальная скорость (км/ч)
    MIN_SPEED_KPH = 10      # Минимальная скорость (км/ч)
    
    # SQL конфигурация (опционально)
    DB_PATH = BASE_DIR / "logistics.db"
    DB_URL = f"sqlite:///{DB_PATH}"
