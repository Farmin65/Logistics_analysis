
"""
Модуль для выявления аномалий в логистических данных
"""
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import Config


class AnomalyDetector:
    """Класс для обнаружения аномалий в маршрутах"""
    
    def __init__(self, data):
        self.data = data
        self.config = Config()
        self.anomalies = {}
        
    def detect_by_statistical_methods(self):
        """Обнаружение аномалий статистическими методами"""
        
        # 1. Z-score метод
        z_scores = np.abs(stats.zscore(self.data[['detour_ratio', 'avg_speed_kph', 'cost_rub']]))
        z_score_anomalies = (z_scores > self.config.ANOMALY_THRESHOLD_STD).any(axis=1)
        
        # 2. IQR метод
        def iqr_outliers(column):
            Q1 = self.data[column].quantile(0.25)
            Q3 = self.data[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - self.config.ANOMALY_THRESHOLD_IQR * IQR
            upper_bound = Q3 + self.config.ANOMALY_THRESHOLD_IQR * IQR
            return (self.data[column] < lower_bound) | (self.data[column] > upper_bound)
        
        iqr_anomalies = (iqr_outliers('detour_ratio') | 
                        iqr_outliers('avg_speed_kph') | 
                        iqr_outliers('cost_rub'))
        
        # 3. Правила предметной области
        domain_anomalies = (
            (self.data['detour_ratio'] > self.config.MAX_DETOUR_RATIO) |
            (self.data['avg_speed_kph'] > self.config.MAX_SPEED_KPH) |
            (self.data['avg_speed_kph'] < self.config.MIN_SPEED_KPH) |
            (self.data['route_efficiency'] < 0.6)
        )
        
        self.anomalies['z_score'] = z_score_anomalies
        self.anomalies['iqr'] = iqr_anomalies
        self.anomalies['domain'] = domain_anomalies
        
        # Комбинированная аномалия (минимум 2 метода подтверждают)
        combined = (z_score_anomalies.astype(int) + 
                   iqr_anomalies.astype(int) + 
                   domain_anomalies.astype(int)) >= 2
        
        self.anomalies['combined'] = combined
        
        return self.anomalies
    
    def detect_by_isolation_forest(self, contamination=0.05):
        """Обнаружение аномалий методом Isolation Forest"""
        
        features = ['detour_ratio', 'avg_speed_kph', 'cost_rub', 'optimal_distance_km']
        X = self.data[features].copy()
        
        # Нормализация
        X_norm = (X - X.mean()) / X.std()
        
        iso_forest = IsolationForest(contamination=contamination, random_state=42)
        predictions = iso_forest.fit_predict(X_norm)
        
        self.anomalies['isolation_forest'] = predictions == -1
        
        return self.anomalies['isolation_forest']
    
    def analyze_anomalies_by_city(self):
        """Анализ аномалий по городам"""
        if 'combined' not in self.anomalies:
            self.detect_by_statistical_methods()
        
        self.data['is_anomaly'] = self.anomalies['combined']
        
        # Статистика по городам отправления
        origin_stats = self.data.groupby('origin_city').agg({
            'is_anomaly': ['count', 'sum'],
            'detour_ratio': 'mean',
            'avg_speed_kph': 'mean'
        }).round(2)
        
        origin_stats.columns = ['total_shipments', 'anomalies_count', 'avg_detour', 'avg_speed']
        origin_stats['anomaly_rate'] = (origin_stats['anomalies_count'] / origin_stats['total_shipments'] * 100).round(1)
        
        # Аномальные маршруты с наибольшим отклонением
        worst_routes = self.data[self.data['is_anomaly']].nlargest(10, 'detour_ratio')[
            ['shipment_id', 'origin_city', 'destination_city', 'detour_ratio', 
             'avg_speed_kph', 'cost_rub', 'route_efficiency']
        ]
        
        return {
            'city_statistics': origin_stats,
            'worst_routes': worst_routes,
            'total_anomalies': self.data['is_anomaly'].sum(),
            'anomaly_percentage': (self.data['is_anomaly'].sum() / len(self.data) * 100)
        }
    
    def get_anomaly_summary(self):
        """Получение сводки по аномалиям"""
        if not self.anomalies:
            self.detect_by_statistical_methods()
        
        summary = {}
        for method, mask in self.anomalies.items():
            summary[method] = {
                'count': mask.sum(),
                'percentage': (mask.sum() / len(mask) * 100)
            }
        
        return pd.DataFrame(summary).T
