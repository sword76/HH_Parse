import requests
import json
import time

# HH API Vacancies URL
url = "https://api.hh.ru/vacancies"

def fetch_hh_vac(url: str, page: int):
    query_params = {
        "text": "python AND flask AND SQL",
        "per_page": 100,
        "page": page,
    }
 
    response = requests.get(url, query_params)
    
    if response.status_code != 200:
        print ("Ошибка в запросе")
    
    print(f"Вакансии успешно со страницы {page} получены!")
    result = response.json()
    
    return result

def fetch_all(url: str):
    page = 0

    vacancies_data = []

    while True:
        if page == 10:
            break

        vacancies = fetch_hh_vac(url, page)
        time.sleep(1)

        if len(vacancies["items"]) == 0:
            break
        
        vacancies_data.extend(vacancies["items"])
        page += 1

        

    with open("./data/vacancies_data.json", "w", encoding = "utf-8") as file:
        file.write(json.dumps(vacancies_data, ensure_ascii=False, indent = 2))


def main():
    
    result = fetch_all(url)
    print(result)

if __name__ == "__main__":
    main()