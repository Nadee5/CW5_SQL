import csv
import psycopg2

from file_name_for_tables import EMPLOYERS_CSV, VACANCIES_CSV


class DBManager:
    """Класс для работы с базой данных вакансий."""

    def __init__(self, database='vacancies_hh', host='localhost', user='postgres', password='12345', port='5432'):
        self.connection = psycopg2.connect(database=database, host=host, user=user, password=password, port=port)
        self.cur = None
        self.sql = ''

    def execute_query(self):
        """Выполняет SLQ-запрос."""
        self.cur = self.connection.cursor()
        self.cur.execute(self.sql)
        self.connection.commit()

    def print_query_result(self):
        """Выводит результат запроса на печать."""
        rows = self.cur.fetchall()
        if rows:
            for row in rows:
                print(*row)
        else:
            print("По Вашему запросу ничего не найдено.")
        self.cur.close()

    def create_table_employers(self) -> None:
        """Создаёт таблицу работодателей в БД."""
        self.sql = ('CREATE TABLE employers '
                    '(id_company int PRIMARY KEY, '
                    'company_name varchar (50), '
                    'area varchar (100), '
                    'all_open_vacancies int, '
                    'description text, '
                    'hh_url text, '
                    'site_url text);')
        self.execute_query()
        print("Table employers created successfully")
        self.cur.close()

    def create_table_vacancies(self) -> None:
        self.sql = ('CREATE TABLE vacancies'
                    '(id int PRIMARY KEY,'
                    'id_company int,'
                    'name_vacancy text,'
                    'salary int,'
                    'salary_currency varchar(10),'
                    'area varchar(100),'
                    'requirement text,'
                    'responsibility text,'
                    'schedule text,'
                    'experience_name varchar(30),'
                    'url text,'
                    ''
                    'CONSTRAINT fk_vacancies_employers FOREIGN KEY(id_company) '
                    'REFERENCES employers(id_company));')
        self.execute_query()
        print("Table vacancies created successfully")
        self.cur.close()

    def fill_the_table_employers(self) -> None:
        with open(EMPLOYERS_CSV, encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                id_company = row['id_company']
                company_name = row['company_name']
                area = row['area']
                all_open_vacancies = row['open_vacancies']
                description = row['description']
                hh_url = row['hh_url']
                site_url = row['site_url']
                self.sql = "INSERT INTO employers VALUES (%s, '%s', '%s', %s, '%s', '%s', '%s')" % (id_company,
                                                                                                    company_name,
                                                                                                    area,
                                                                                                    all_open_vacancies,
                                                                                                    description,
                                                                                                    hh_url, site_url)
                self.execute_query()
        self.cur.close()

    def fill_the_table_vacancies(self) -> None:
        with (open(VACANCIES_CSV, encoding='utf-8') as csv_file):
            reader = csv.DictReader(csv_file)
            for row in reader:
                id = row['id']
                id_company = row['id_company']
                name_vacancy = row['name_vacancy']
                salary = row['salary']
                salary_currency = row['salary_currency']
                area = row['area']
                requirement = row['requirement']
                responsibility = row['responsibility']
                schedule = row['schedule']
                experience_name = row['experience_name']
                url = row['url']
                self.sql = ("INSERT INTO vacancies "
                            "VALUES (%s, %s, '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')") %(id, id_company,
                                                                                                     name_vacancy,
                                                                                                     salary,
                                                                                                     salary_currency,
                                                                                                     area, requirement,
                                                                                                     responsibility,
                                                                                                     schedule,
                                                                                                     experience_name,
                                                                                                     url)
                self.execute_query()
        self.cur.close()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании.
        (количество вакансий, название компании)."""
        self.sql = ('SELECT COUNT(*), employers.company_name '
                    'FROM vacancies '
                    'JOIN employers USING(id_company) '
                    'GROUP BY company_name')
        self.execute_query()
        self.print_query_result()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        (название компании, вакансия, заработная плата от, заработная плата до, валюта, ссылка на вакансию на hh.)"""
        self.sql = ('SELECT employers.company_name, vacancies.name_vacancy, '
                    'vacancies.salary, vacancies.salary_currency, '
                    'vacancies.url FROM vacancies '
                    'JOIN employers USING(id_company)')
        self.execute_query()
        self.print_query_result()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям.
        (название компании, средняя заработная плата.)"""
        self.sql = ('SELECT employers.company_name, ROUND(AVG(vacancies.salary), 0) AS average_salary, '
                    'vacancies.salary_currency '
                    'FROM vacancies '
                    'JOIN employers USING(id_company) '
                    'GROUP BY employers.company_name, vacancies.salary_currency '
                    'ORDER BY employers.company_name')
        self.execute_query()
        self.print_query_result()

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        (все вакансии с фильтром >AGV, в валюте RUR)"""
        self.sql = ('SELECT * FROM vacancies '
                    'WHERE salary > (SELECT AVG(salary) FROM vacancies) AND salary_currency IN (\'RUR\')')
        self.execute_query()
        self.print_query_result()

    def get_vacancies_with_keyword(self, keyword: str):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
        :param keyword: Слово-фильтр для названия вакансий.
        :return: Отфильтрованный список вакансий.
        """
        self.sql = f'SELECT * FROM vacancies WHERE name_vacancy LIKE \'%{keyword}%\';'
        self.execute_query()
        self.print_query_result()
