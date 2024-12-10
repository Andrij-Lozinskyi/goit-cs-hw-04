import threading
from pathlib import Path
import time
from typing import List, Dict, Set
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

class TextSearcher:
    def __init__(self, keywords: List[str], num_threads: int = 4):
        self.keywords = set(keywords)
        self.num_threads = num_threads
        self.results = defaultdict(set)
        self.results_lock = threading.Lock()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('TextSearcher')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def search_file(self, filepath: Path) -> Dict[str, Set[str]]:
        local_results = defaultdict(set)
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read().lower()
                for keyword in self.keywords:
                    if keyword.lower() in content:
                        local_results[keyword].add(str(filepath))
        except Exception as e:
            self.logger.error(f"Помилка при обробці файлу {filepath}: {str(e)}")
        return local_results

    def process_files(self, files: List[Path]):
        for filepath in files:
            local_results = self.search_file(filepath)
            with self.results_lock:
                for keyword, files in local_results.items():
                    self.results[keyword].update(files)

    def search_directory(self, directory: str, file_pattern: str = "*.txt") -> Dict[str, Set[str]]:
        start_time = time.time()
        
        try:
            path = Path(directory)
            all_files = list(path.glob(file_pattern))
            
            if not all_files:
                self.logger.warning(f"Не знайдено файлів з шаблоном {file_pattern} в директорії {directory}")
                return dict(self.results)

            files_per_thread = len(all_files) // self.num_threads + 1
            file_chunks = [all_files[i:i + files_per_thread] 
                         for i in range(0, len(all_files), files_per_thread)]

            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                executor.map(self.process_files, file_chunks)

            execution_time = time.time() - start_time
            self.logger.info(f"Пошук завершено за {execution_time:.2f} секунд")
            self.logger.info(f"Оброблено файлів: {len(all_files)}")
            
            return dict(self.results)

        except Exception as e:
            self.logger.error(f"Помилка при скануванні директорії: {str(e)}")
            return dict(self.results)

def get_keywords_from_user() -> List[str]:
    print("Введіть слова натискаючи Enter після кожного.")
    print("Щоб завершити введення, двічі натисніть Enter.\n")
    
    keywords = []
    while True:
        keyword = input("Введіть ключове слово: ").strip()
        if not keyword:
            if keywords:
                break
            print("Будь ласка, введіть хоча б одне ключове слово!")
            continue
        keywords.append(keyword)
    
    return keywords

def get_directory_from_user() -> str:
    while True:
        directory = input("\nВведіть шлях до директорії для пошуку: ").strip()
        if directory and Path(directory).exists():
            return directory
        print("Помилка: Шлях не існує. Спробуйте ще раз.")

def main():
    try:
        keywords = get_keywords_from_user()
        directory = get_directory_from_user()
        searcher = TextSearcher(keywords, num_threads=4)
        results = searcher.search_directory(directory, "*.txt")
        
        print("\nРезультати пошуку:")
        if not any(results.values()):
            print("Нічого не знайдено.")
        else:
            for keyword, files in results.items():
                if files: 
                    print(f"\nСлово '{keyword}' знайдено у файлах:")
                    for file in files:
                        print(f"- {file}")
                        
    except KeyboardInterrupt:
        print("\nПошук перервано.")
    except Exception as e:
        print(f"Помилка при виконанні: {str(e)}")

if __name__ == "__main__":
    main()
    