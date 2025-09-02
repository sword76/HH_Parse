import requests
import json
import time
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
from functools import wraps

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# HH API Vacancies URL
url = "https://api.hh.ru/vacancies"


# Выполнения запросов с повторениям с использования декоратора
def retry_request(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    retryable_status_codes: List[int] = [429, 500, 502, 503, 504]
):
    def decorator(func: callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            
            while retries <= max_retries:
                try:
                    result = func(*args, **kwargs)
                    
                    # Если функция возвращает response объект
                    if hasattr(result, 'status_code'):
                        if result.status_code == 200:
                            return result
                        elif result.status_code in retryable_status_codes:
                            logger.warning(f"Получен статус {result.status_code}, попытка {retries + 1}/{max_retries}")
                        else:
                            return result  # Не retry-able ошибка
                    else:
                        return result  # Если функция возвращает не response
                        
                except (requests.exceptions.RequestException, 
                       requests.exceptions.Timeout,
                       requests.exceptions.ConnectionError) as e:
                    logger.warning(f"Ошибка сети: {e}, попытка {retries + 1}/{max_retries}")
                
                # Если достигли максимума попыток, выходим
                if retries == max_retries:
                    logger.error(f"Превышено максимальное количество попыток ({max_retries})")
                    return None
                
                # Ждем перед следующей попыткой (экспоненциальная backoff задержка)
                time.sleep(delay)
                delay *= backoff_factor
                retries += 1
                
            return None
        return wrapper
    return decorator


# Выполнение HTTP запроса с retry
@retry_request(max_retries=3, initial_delay=1.0, backoff_factor=2.0)
def make_request(url: str, params: Dict[str, Any]) -> Optional[requests.Response]:
    return requests.get(url, params=params, timeout=10)


# Функция получения вакансий с HH API
def fetch_hh_vac(url: str, page: int) -> Optional[Dict[str, Any]]:

    query_params = {
        "text": "python AND flask AND SQL",
        "per_page": 100,
        "page": page,
        "area": 1,  # Москва
        "only_with_salary": True,
    }
    
    try:
        response = make_request(url, query_params)
        
        if not response:
            logger.error(f"Не удалось выполнить запрос для страницы {page}")
            return None
        
        if response.status_code != 200:
            logger.error(f"Ошибка HTTP {response.status_code} для страницы {page}")
            return None
        
        logger.info(f"Вакансии успешно со страницы {page} получены!")
        return response.json()
        
    except requests.exceptions.Timeout:
        logger.error(f"Таймаут запроса для страницы {page}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON для страницы {page}: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка для страницы {page}: {e}")
        return None


# Функция фильтрации вакансии по мин. зарплате
def filter_by_salary(vacancies: List[Dict[str, Any]], min_salary: int) -> List[Dict[str, Any]]:

    filtered = []
    for vacancy in vacancies:
        salary = vacancy.get('salary')
        if salary:
            # Проверяем обе границы зарплаты (from и to)
            salary_from = salary.get('from')
            salary_to = salary.get('to')
            
            if (salary_from and salary_from >= min_salary) or \
               (salary_to and salary_to >= min_salary):
                filtered.append(vacancy)
    
    return filtered

# Запрос всех вакансий с фильтром по зарплате
def fetch_all(url: str, min_salary: int = 250000) -> List[Dict[str, Any]]:
    
    page = 0
    all_vacancies = []

    while True:
        vacancies = fetch_hh_vac(url, page)
        
        if not vacancies or 'items' not in vacancies:
            logger.warning(f"Не удалось получить данные для страницы {page}")
            break
            
        current_vacancies = vacancies.get('items', [])
        if not current_vacancies:
            logger.warning(f"Отсутствует ключ 'items' в ответе для страницы {page}")
            break

        # Фильтруем по зарплате
        filtered_vacancies = filter_by_salary(current_vacancies, min_salary)
        all_vacancies.extend(filtered_vacancies)

        # Проверяем, есть ли следующая страница
        pages = vacancies.get('pages', 0)
        if page >= pages - 1 or page >= 19:  # HH API ограничивает 2000 вакансий (20 страниц)
            logger.info(f"Достигнут предел страниц ({pages})")
            break
            
        page += 1
        time.sleep(0.2)  # Более безопасная задержка

    return all_vacancies

# Функиция сохранения данных в файл
def save_to_file(vacancies: List[Dict[str, Any]], filename: str = "./data/vacancies_data.json") -> None:
    try:
        # Создаём директорию, если она не существует
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(vacancies, file, ensure_ascii=False, indent=2)
        
        logging.info(f"Данные успешно сохранены в {filename}")
    
    except IOError as e:
        logging.info(f"Ошибка при сохранении файла: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при сохранении: {e}")

# Основная функция сбора данных по вакансиям
def main():
    logger.info("Начинаем сбор вакансий...")
    
    vacancies = fetch_all(url, min_salary=250000)
    
    if vacancies:
        logger.info(f"Найдено {len(vacancies)} вакансий с зарплатой от 250000 руб.")
        save_to_file(vacancies)
    else:
        logger.warning("Не удалось получить вакансии или подходящие вакансии не найдены.")


if __name__ == "__main__":
    main()