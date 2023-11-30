
import psycopg2
import os

from config import config
from constants_file_name import EMPLOYERS_JSON, EMPLOYERS_CSV, VACANCIES_JSON, VACANCIES_CSV
from utils import save_data_to_json, convert_file_from_json_to_csv
from dbmanager import DBManager


"""Устанавливаем стартовые параметры соединения, имя базы данных и таблиц."""

params = config()
db_name = 'vacancies_hh'

connection = psycopg2.connect(**params)
cur = connection.cursor()
connection.autocommit = True


"""Создаём базу данных с заданным именем, разрываем соединение."""

cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
cur.execute(f"CREATE DATABASE {db_name} "
            f"WITH OWNER = postgres "
            f"ENCODING = 'utf8' "
            f"CONNECTION LIMIT = -1 IS_TEMPLATE = False;")
connection.commit()
cur.close()
connection.close()
params.update(database=db_name)


"""Сохраняем полученные по API данные в json, конвертируем в csv."""

if os.path.exists(EMPLOYERS_JSON):
    os.remove(EMPLOYERS_JSON)
save_data_to_json(filename=EMPLOYERS_JSON)
convert_file_from_json_to_csv(file_in=EMPLOYERS_JSON, file_out=EMPLOYERS_CSV)

if os.path.exists(VACANCIES_JSON):
    os.remove(VACANCIES_JSON)
save_data_to_json(filename=VACANCIES_JSON)
convert_file_from_json_to_csv(file_in=VACANCIES_JSON, file_out=VACANCIES_CSV)


"""Создаём экземпляр класса DBManager для подключения к БД и работы с ней."""

db = DBManager()
db.create_table_employers()
db.fill_the_table_employers()
db.create_table_vacancies()
db.fill_the_table_vacancies()


def request_to_database(db_object):
    """Выполняет запрос пользователя к БД.
    В качестве аргумента получает экземпляр класса DBManager."""

    while True:
        print("Выберите действие:")
        print("1 - Получить список всех компаний и количество вакансий у каждой компании")
        print("2 - Получить список всех вакансий с указанием названия компании, "
              "названия вакансии и зарплаты и ссылки на вакансию")
        print("3 - Получить среднюю зарплату по вакансиям")
        print("4 - Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям (в рублях).")
        print("5 - Получить список всех вакансий по ключевому слову в названии вакансии.")
        print("0 - Завершить работу.")

        answer = int(input())
        if answer == 0:
            print("Работа программы завершена.")
            break
        elif answer == 1:
            db_object.get_companies_and_vacancies_count()
        elif answer == 2:
            db_object.get_all_vacancies()
        elif answer == 3:
            db_object.get_avg_salary()
        elif answer == 4:
            db_object.get_vacancies_with_higher_salary()
        elif answer == 5:
            print("Введите слово для поиска:")
            keyword = input()
            db_object.get_vacancies_with_keyword(keyword)
        else:
            print("Некорректный ввод.")


request_to_database(db)
db.connection.close()
