import requests
import json
import time
from typing import List, Dict, Any, Optional

# HH API Vacancies URL
url = "https://api.hh.ru/vacancies"


# Функция получения вакансий с HH API
def fetch_hh_vac(url: str, page: int) -> Optional[Dict[str, Any]]:
    """Получение вакансий с HH API"""
    query_params = {
        "text": "python AND flask AND SQL",
        "per_page": 100,
        "page": page,
        "area": 1,  # Москва
        "only_with_salary": True,
    }
    
    try:
        response = requests.get(url, params=query_params, timeout=10)
        response.raise_for_status()  # Вызовет исключение для кодов 4xx/5xx
        
        print(f"Вакансии успешно со страницы {page} получены!")
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Ошибка парсинга JSON: {e}")
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
            break
            
        current_vacancies = vacancies.get('items', [])
        if not current_vacancies:
            break

        # Фильтруем по зарплате
        filtered_vacancies = filter_by_salary(current_vacancies, min_salary)
        all_vacancies.extend(filtered_vacancies)

        # Проверяем, есть ли следующая страница
        pages = vacancies.get('pages', 0)
        if page >= pages - 1 or page >= 19:  # HH API ограничивает 2000 вакансий (20 страниц)
            break
            
        page += 1
        time.sleep(0.25)  # Более безопасная задержка

    return all_vacancies

# Функиция сохранения данных в файл
def save_to_file(vacancies: List[Dict[str, Any]], filename: str = "./data/vacancies_data.json") -> None:
   
    try:
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(vacancies, file, ensure_ascii=False, indent=2)
        print(f"Данные успешно сохранены в {filename}")
    except IOError as e:
        print(f"Ошибка при сохранении файла: {e}")


def main():
    # Основная функция сбора данных по вакансиям
    print("Начинаем сбор вакансий...")
    
    vacancies = fetch_all(url, min_salary=250000)
    
    if vacancies:
        print(f"Найдено {len(vacancies)} вакансий с зарплатой от 250000 руб.")
        save_to_file(vacancies)
    else:
        print("Не удалось получить вакансии или подходящие вакансии не найдены.")


if __name__ == "__main__":
    main()