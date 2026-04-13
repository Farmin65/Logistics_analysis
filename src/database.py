
"""
Модуль для работы с SQL базой данных
"""
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import Config


class LogisticsDatabase:
    """Класс для работы с SQL базой данных логистики"""
    
    def __init__(self):
        self.config = Config()
        self.engine = create_engine(self.config.DB_URL)
        self.connection = None
        
    def create_tables(self):
        """Создание таблиц в базе данных"""
        
        create_shipments_table = """
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id TEXT PRIMARY KEY,
            origin_city TEXT NOT NULL,
            destination_city TEXT NOT NULL,
            optimal_distance_km REAL,
            actual_distance_km REAL,
            travel_time_hours REAL,
            cost_rub REAL,
            avg_speed_kph REAL,
            detour_ratio REAL,
            date DATE,
            day_of_week TEXT,
            month INTEGER,
            is_weekend INTEGER,
            route_efficiency REAL,
            is_anomaly INTEGER DEFAULT 0
        );
        """
        
        create_anomalies_table = """
        CREATE TABLE IF NOT EXISTS anomalies_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shipment_id TEXT,
            detection_method TEXT,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            anomaly_score REAL,
            FOREIGN KEY (shipment_id) REFERENCES shipments(shipment_id)
        );
        """
        
        create_routes_stats_table = """
        CREATE TABLE IF NOT EXISTS routes_statistics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_city TEXT,
            destination_city TEXT,
            total_shipments INTEGER,
            avg_detour_ratio REAL,
            avg_travel_time REAL,
            anomaly_count INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(origin_city, destination_city)
        );
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_shipments_table))
            conn.execute(text(create_anomalies_table))
            conn.execute(text(create_routes_stats_table))
            conn.commit()
            
        print("✅ Таблицы успешно созданы")
        
    def insert_shipments(self, df):
        """Вставка данных о перевозках"""
        df.to_sql('shipments', self.engine, if_exists='replace', index=False)
        print(f"✅ Вставлено {len(df)} записей в таблицу shipments")
        
    def update_anomalies(self, anomaly_mask, detection_method='combined'):
        """Обновление статуса аномалий"""
        anomaly_shipments = anomaly_mask[anomaly_mask].index.tolist()
        
        with self.engine.connect() as conn:
            # Обновляем is_anomaly в shipments
            for shipment_id in anomaly_shipments:
                conn.execute(
                    text("UPDATE shipments SET is_anomaly = 1 WHERE shipment_id = :shipment_id"),
                    {"shipment_id": shipment_id}
                )
            
            # Логируем аномалии
            for shipment_id in anomaly_shipments:
                conn.execute(
                    text("""
                        INSERT INTO anomalies_log (shipment_id, detection_method) 
                        VALUES (:shipment_id, :method)
                    """),
                    {"shipment_id": shipment_id, "method": detection_method}
                )
            conn.commit()
            
        print(f"✅ Отмечено {len(anomaly_shipments)} аномальных маршрутов")
        
    def update_route_statistics(self, df):
        """Обновление статистики по маршрутам"""
        
        route_stats = df.groupby(['origin_city', 'destination_city']).agg({
            'shipment_id': 'count',
            'detour_ratio': 'mean',
            'travel_time_hours': 'mean',
            'is_anomaly': 'sum'
        }).reset_index()
        
        route_stats.columns = ['origin_city', 'destination_city', 'total_shipments', 
                               'avg_detour_ratio', 'avg_travel_time', 'anomaly_count']
        
        with self.engine.connect() as conn:
            # Очищаем таблицу
            conn.execute(text("DELETE FROM routes_statistics"))
            
            # Вставляем новые данные
            route_stats.to_sql('routes_statistics', conn, if_exists='append', index=False)
            conn.commit()
            
        print("✅ Статистика маршрутов обновлена")
        
    def get_top_anomalous_routes(self, limit=10):
        """Получение самых аномальных маршрутов"""
        query = f"""
        SELECT 
            origin_city,
            destination_city,
            total_shipments,
            anomaly_count,
            ROUND(CAST(anomaly_count AS FLOAT) / total_shipments * 100, 2) as anomaly_rate,
            ROUND(avg_detour_ratio, 2) as avg_detour
        FROM routes_statistics
        WHERE anomaly_count > 0
        ORDER BY anomaly_rate DESC
        LIMIT {limit}
        """
        
        return pd.read_sql(query, self.engine)
    
    def get_detailed_anomaly_report(self):
        """Детальный отчет по аномалиям"""
        query = """
        SELECT 
            s.shipment_id,
            s.origin_city,
            s.destination_city,
            s.detour_ratio,
            s.avg_speed_kph,
            s.cost_rub,
            s.date,
            a.detection_method,
            a.detected_at,
            a.anomaly_score
        FROM shipments s
        JOIN anomalies_log a ON s.shipment_id = a.shipment_id
        ORDER BY s.detour_ratio DESC
        LIMIT 20
        """
        
        return pd.read_sql(query, self.engine)
    
    def close(self):
        """Закрытие соединения"""
        if self.connection:
            self.connection.close()
