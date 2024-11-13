import os
import time
import random
import string
import plyvel
import statistics
import matplotlib.pyplot as plt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
from dataclasses import dataclass
from typing import List, Dict
import numpy as np

@dataclass
class BenchmarkConfig:
    num_files: int = 1000
    file_size: int = 1024
    num_workers: int = 8
    concurrent_requests: int = 20
    base_path: str = "/tmp/benchmark"

class StorageBenchmark:
    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.leveldb_path = os.path.join(config.base_path, "leveldb")
        self.ext4_path = os.path.join(config.base_path, "ext4")
        self.results = {}
        self.task_queue = Queue()
        self.result_queue = Queue()
        
        # Thread-local storage для LevelDB соединений
        self.thread_local = threading.local()
        
        # Создаем директории если не существуют
        Path(self.ext4_path).mkdir(parents=True, exist_ok=True)
        
    def get_db_connection(self):
        """Получает или создает thread-local соединение с LevelDB"""
        if not hasattr(self.thread_local, "db"):
            self.thread_local.db = plyvel.DB(self.leveldb_path, create_if_missing=True)
        return self.thread_local.db

    def generate_random_data(self, size):
        """Генерирует случайные данные указанного размера"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    
    def worker(self):
        """Функция воркера для обработки задач из очереди"""
        while True:
            task = self.task_queue.get()
            if task is None:
                break
                
            operation, storage_type, index = task
            result = self.process_task(operation, storage_type, index)
            self.result_queue.put(result)
            self.task_queue.task_done()
    
    def process_task(self, operation, storage_type, index):
        """Обрабатывает одну задачу тестирования"""
        if operation == 'write':
            data = self.generate_random_data(self.config.file_size)
            start_time = time.time()
            
            if storage_type == 'leveldb':
                db = self.get_db_connection()
                key = f"key_{index}".encode()
                db.put(key, data.encode())
            else:  # ext4
                filepath = os.path.join(self.ext4_path, f"file_{index}")
                with open(filepath, 'w') as f:
                    f.write(data)
                    
        else:  # read
            start_time = time.time()
            
            if storage_type == 'leveldb':
                db = self.get_db_connection()
                key = f"key_{index}".encode()
                _ = db.get(key)
            else:  # ext4
                filepath = os.path.join(self.ext4_path, f"file_{index}")
                with open(filepath, 'r') as f:
                    _ = f.read()
                    
        end_time = time.time()
        return (operation, storage_type, end_time - start_time)
    
    def run_parallel_benchmark(self, operation: str, storage_type: str) -> List[float]:
        """Запускает параллельное тестирование для заданной операции и типа хранилища"""
        # Заполняем очередь задач
        for i in range(self.config.num_files):
            self.task_queue.put((operation, storage_type, i))
            
        # Добавляем сигналы завершения для воркеров
        for _ in range(self.config.num_workers):
            self.task_queue.put(None)
            
        # Запускаем воркеры
        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            workers = [executor.submit(self.worker) for _ in range(self.config.num_workers)]
            
        # Собираем результаты
        results = []
        while len(results) < self.config.num_files:
            result = self.result_queue.get()
            results.append(result[2])  # Добавляем только время выполнения
            
        return results

    def run_benchmark(self):
        """Запускает полное тестирование"""
        print(f"Запуск бенчмарка с конфигурацией:")
        print(f"Файлов: {self.config.num_files}")
        print(f"Размер файла: {self.config.file_size} байт")
        print(f"Воркеров: {self.config.num_workers}")
        print(f"Одновременных запросов: {self.config.concurrent_requests}")
        
        self.results = {
            'write': {
                'leveldb': self.run_parallel_benchmark('write', 'leveldb'),
                'ext4': self.run_parallel_benchmark('write', 'ext4')
            },
            'read': {
                'leveldb': self.run_parallel_benchmark('read', 'leveldb'),
                'ext4': self.run_parallel_benchmark('read', 'ext4')
            }
        }
        
        self.print_results()
        self.plot_results()
        
    def calculate_percentiles(self, times: List[float]) -> Dict[str, float]:
        """Вычисляет перцентили времени выполнения"""
        return {
            'p50': np.percentile(times, 50),
            'p95': np.percentile(times, 95),
            'p99': np.percentile(times, 99)
        }
    
    def print_results(self):
        """Выводит результаты тестирования"""
        print("\nРезультаты бенчмарка:")
        
        for operation in ['write', 'read']:
            print(f"\n{operation.capitalize()}:")
            for storage in ['leveldb', 'ext4']:
                times = self.results[operation][storage]
                avg_time = statistics.mean(times)
                std_dev = statistics.stdev(times)
                percentiles = self.calculate_percentiles(times)
                throughput = self.config.num_files / sum(times)
                
                print(f"{storage}:")
                print(f"  Среднее время: {avg_time:.6f} сек")
                print(f"  Стандартное отклонение: {std_dev:.6f} сек")
                print(f"  Медиана (P50): {percentiles['p50']:.6f} сек")
                print(f"  P95: {percentiles['p95']:.6f} сек")
                print(f"  P99: {percentiles['p99']:.6f} сек")
                print(f"  Пропускная способность: {throughput:.2f} операций/сек")
    
    def plot_results(self):
        """Создает графики результатов"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # График времени записи
        ax1.boxplot([self.results['write']['leveldb'], self.results['write']['ext4']], 
                   labels=['LevelDB', 'Ext4'])
        ax1.set_title('Время записи')
        ax1.set_ylabel('Время (секунды)')
        
        # График времени чтения
        ax2.boxplot([self.results['read']['leveldb'], self.results['read']['ext4']], 
                   labels=['LevelDB', 'Ext4'])
        ax2.set_title('Время чтения')
        ax2.set_ylabel('Время (секунды)')
        
        # График распределения времени записи
        for storage, color in zip(['leveldb', 'ext4'], ['blue', 'orange']):
            ax3.hist(self.results['write'][storage], bins=50, alpha=0.5, 
                    label=storage, color=color)
        ax3.set_title('Распределение времени записи')
        ax3.set_xlabel('Время (секунды)')
        ax3.legend()
        
        # График распределения времени чтения
        for storage, color in zip(['leveldb', 'ext4'], ['blue', 'orange']):
            ax4.hist(self.results['read'][storage], bins=50, alpha=0.5, 
                    label=storage, color=color)
        ax4.set_title('Распределение времени чтения')
        ax4.set_xlabel('Время (секунды)')
        ax4.legend()
        
        plt.tight_layout()
        plt.savefig('benchmark_results.png')
        plt.close()

if __name__ == "__main__":
    # Пример использования с настройкой параметров
    config = BenchmarkConfig(
        num_files=1000,
        file_size=1024,
        num_workers=4,
        concurrent_requests=20
    )
    
    benchmark = StorageBenchmark(config)
    benchmark.run_benchmark()
