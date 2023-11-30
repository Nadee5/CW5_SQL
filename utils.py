import requests
import json
import pandas as pd

from constants_file_name import EMPLOYERS_JSON, VACANCIES_JSON

employers = {
    'Cбер': '3529',
    'Яндекс': '1740',
    'Альфа-Банк': '80',
    'VK': '15478',
    'Тинькофф': '78638',
    'Газпром нефть': '39305',
    'ВТБ': '4181',
    'СИБУР': '3809',
    'МТС': '3776',
    'OZON': '2180',
}


def get_data_employers():
    """Получает данные о работодателях по API, обрабатывает полученные данные."""
    universal_lst_emps = []
    for employer_id in employers.values():
        url = f'https://api.hh.ru/employers/{employer_id}'
        data = requests.get(url).json()
        universal_dict_employer = {
            'id_company': data['id'],
            'company_name': data['name'],
            'area': data['area']['name'],
            'open_vacancies': data['open_vacancies'],
            'description': data['description'],
            'hh_url': data['alternate_url'],
            'site_url': data['site_url'],
        }
        universal_lst_emps.append(universal_dict_employer)
    return universal_lst_emps


def get_data_vacancies():
    """Получает данные о пагинации вакансий каждой компании из списка,
        запускает цикл итерации по страницам с вакансиями каждой компании,
        составляет список словарей с универсальными полями данных."""
    universal_lst_vacs = []
    current_id = 1
    for employer_id in employers.values():
        url = f'https://api.hh.ru/vacancies?employer_id={employer_id}&only_with_salary=true&page=0&per_page=100'
        data = requests.get(url).json()  # json словарь
        list_of_vacancies = data['items']  # все items
        pages = data['pages']
        for i in range(pages + 1):
            for dict_ in list_of_vacancies:
                salary_from = dict_['salary'].get('from')
                salary_to = dict_['salary'].get('to')
                if salary_from is not None and salary_to is not None:
                    salary = (salary_from+salary_to)/2
                elif salary_from is not None:
                    salary = salary_from
                elif salary_to is not None:
                    salary = salary_to
                else:
                    print('[INFO] что-то пошло не так')
                universal_dict = {
                    'id': current_id,
                    'id_company': employer_id,
                    'name_vacancy': dict_['name'],
                    'salary': salary,
                    'salary_currency': dict_['salary'].get('currency'),
                    'area': dict_['area']['name'],
                    'requirement': dict_['snippet']['requirement'],
                    'responsibility': dict_['snippet']['responsibility'],
                    'schedule': dict_['schedule']['name'],
                    'experience_name': dict_['experience']['name'],
                    'url': dict_['alternate_url'],
                }
                universal_lst_vacs.append(universal_dict)
                current_id += 1
    return universal_lst_vacs


def save_data_to_json(filename):
    """Сохраняет информацию в файл json."""
    if filename == EMPLOYERS_JSON:
        universal_lst = get_data_employers()
    elif filename == VACANCIES_JSON:
        universal_lst = get_data_vacancies()
    else:
        print("Нет такого файла.")
    with open(filename, 'a', encoding='utf-8') as file:
        json.dump(universal_lst, file, ensure_ascii=False, indent=4)


def convert_file_from_json_to_csv(file_in, file_out):
    with open(file_in, encoding='utf-8') as inputfile:
        df = pd.read_json(inputfile)
    df.to_csv(file_out, encoding='utf-8', index=False)
