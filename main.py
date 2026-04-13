
"""
Главный исполняемый файл проекта
Анализ логистических данных и выявление аномалий
"""
import sys
import warnings
warnings.filterwarnings('ignore')

from src.data_loader import DataLoader
from src.anomaly_detector import AnomalyDetector
from src.visualizer import Visualizer
from src.database import LogisticsDatabase


def main():
    print("=" * 60)
    print("🚚 АНАЛИЗ ЛОГИСТИЧЕСКИХ ДАННЫХ И ВЫЯВЛЕНИЕ АНОМАЛИЙ")
    print("=" * 60)
    
    # 1. Загрузка и подготовка данных
    print("\n📂 Шаг 1: Загрузка данных...")
    loader = DataLoader()
    
    # Генерируем пример данных (в реальном проекте загружаем из файла)
    loader.generate_sample_data(n_rows=2000)
    
    # Загрузка и предобработка
    data = loader.load_data()
    data = loader.preprocess_data()
    
    print(f"\n📊 Статистика данных:")
    print(f"   - Всего записей: {len(data)}")
    print(f"   - Уникальных маршрутов: {data.groupby(['origin_city', 'destination_city']).ngroups}")
    print(f"   - Период: {data['date'].min().date()} - {data['date'].max().date()}")
    
    # 2. Выявление аномалий
    print("\n🔍 Шаг 2: Выявление аномальных маршрутов...")
    detector = AnomalyDetector(data)
    
    # Статистические методы
    anomalies = detector.detect_by_statistical_methods()
    
    # Isolation Forest (опционально)
    iso_anomalies = detector.detect_by_isolation_forest(contamination=0.05)
    
    # Анализ результатов
    anomaly_summary = detector.get_anomaly_summary()
    print("\n📈 Результаты обнаружения аномалий:")
    print(anomaly_summary.to_string())
    
    # Детальный анализ
    analysis_results = detector.analyze_anomalies_by_city()
    print(f"\n📊 Общая статистика:")
    print(f"   - Всего аномалий: {analysis_results['total_anomalies']}")
    print(f"   - Доля аномалий: {analysis_results['anomaly_percentage']:.2f}%")
    
    print("\n🏙️ Статистика по городам (ТОП-5 по аномалиям):")
    print(analysis_results['city_statistics'].nlargest(5, 'anomaly_rate').to_string())
    
    print("\n⚠️ 10 самых проблемных маршрутов:")
    print
