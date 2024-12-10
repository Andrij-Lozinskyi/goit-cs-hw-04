import multiprocessing as mp
from pathlib import Path
import time
from typing import List, Dict, Set
import logging
from collections import defaultdict

class TextSearcher:
    def __init__(self, keywords: List[str], num_processes: int = None):
        self.keywords = set(keywords)
        self.num_processes = num_processes or mp.cpu_count()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('TextSearcher')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    @staticmethod
    def search_file(filepath: Path, keywords: Set[str]) -> Dict[str, Set[str]]:
        local_results = defaultdict(set)
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read().lower()
                for keyword in keywords:
                    if keyword.lower() in content:
                        local_results[keyword].add(str(filepath))
        except Exception as e:
            print(f"Помилка при обробці файлу {filepath}: {str(e)}")
        return dict(local_results)

    @staticmethod
    def process_files(files: List[Path], keywords: Set[str], result_queue: mp.Queue):
        process_results = defaultdict(set)
        
        for filepath in files:
            file_results = TextSearcher.search_file(filepath, keywords)
            for keyword, files in file_results.items():
                process_results[keyword].update(files)
        
        result_queue.put(dict(process_results))

    def search_directory(self, directory: str, file_pattern: str = "*.txt") -> Dict[str, Set[str]]:
        start_time = time.time()
        
        try:
            path = Path(directory)
            all_files = list(path.glob(file_pattern))
            
            if not all_files:
                self.logger.warning(f"Не знайдено файлів з шаблоном {file_pattern} в директорії {directory}")
                return {}

            # Розділяємо файли між процесами
            files_per_process = len(all_files) // self.num_processes + 1
            file_chunks = [all_files[i:i + files_per_process] 
                         for i in range(0, len(all_files), files_per_process)]

            result_queue = mp.Queue()
            
            processes = []
            for chunk in file_chunks:
                p = mp.Process(
                    target=self.process_files,
                    args=(chunk, self.keywords, result_queue)
                )
                processes.append(p)
                p.start()

            final_results = defaultdict(set)
            for _ in range(len(processes)):
                process_results = result_queue.get()
                for keyword, files in process_results.items():
                    final_results[keyword].update(files)

            for p in processes:
                p.join()

            execution_time = time.time() - start_time
            self.logger.info(f"Пошук завершено за {execution_time:.2f} секунд")
            self.logger.info(f"Оброблено файлів: {len(all_files)}")
            
            return dict(final_results)

        except Exception as e:
            self.logger.error(f"Помилка при скануванні директорії: {str(e)}")
            return {}

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
        mp.freeze_support()
        
        keywords = get_keywords_from_user()
        directory = get_directory_from_user()
        
        searcher = TextSearcher(keywords)
        results = searcher.search_directory(directory, "*.txt")
        
        print("\nРезультати пошуку:")
        if not results:
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
    