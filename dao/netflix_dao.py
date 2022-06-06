import sqlite3
import json
from collections import Counter


class NetflixDAO:
    """
    Класс поиска, сортировки по БД netflix
    Поля: путь к БД netflix
    Методы
    """

    def __init__(self, path):
        """
        :param path: путь к БД
        """
        self.path = path
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.cursor = self.connection.cursor()

    def __del__(self):
        """
        закрывает соединение
        """
        self.cursor.close()
        self.connection.close()

    def get_result_by_title(self, title):
        """
        Передает результат поиска по названию фильма
        :param title: название фильма для поиска по БД
        :return: словарь результат поиска
        """
        self.cursor.execute(f"""
                                SELECT title, country, release_year, listed_in, description 
                                FROM netflix
                                WHERE title = '{title}'
                                ORDER BY release_year DESC
                                LIMIT 1
                            """)
        result = self.cursor.fetchone()
        list_result = {"title": result[0],
                       "country": result[1],
                       "release_year": result[2],
                       "genre": result[3],
                       "description": result[4].rstrip('\n')}
        return json.dumps(list_result)

    def get_result_by_years(self, year_1, year_2):
        """
        Переает результат поиска фильмов по диапазону годов
        :param year_1: год выхода фильма
        :param year_2: год выхода фильма
        :return: список словарей результат поиска
        """
        self.cursor.execute(f"""
                                SELECT title, release_year
                                FROM netflix
                                WHERE release_year BETWEEN {year_1} AND {year_2} 
                                LIMIT 100
                            """)
        list_result = [{"title": _[0],
                        "release_year": _[1]}
                       for _ in self.cursor.fetchall()]
        return json.dumps(list_result)

    def get_result_by_rating(self, category):
        """
        Выдача результата поиска фильмов по рейтингу возрастных ограничений
        :param category: категория фильмов
        :return: список словарей результат поиска по рейтингу
        """
        rating_dict = {"children": "'G'",
                       "family": "'G', 'PG', 'PG-13'",
                       "adult": "'R', 'NC-17'"}
        self.cursor.execute(f"""
                                SELECT title, rating, description
                                FROM netflix
                                WHERE rating in ({rating_dict[category]})
                                GROUP BY title, rating, description
                                LIMIT 100
                            """)
        result = self.cursor.fetchall()
        list_result = [{"title": _[0],
                        "rating": _[1],
                        "description": _[2].rstrip('\n')} for _ in result]
        return json.dumps(list_result)

    def get_result_by_genre(self, genre):
        """
        Передает результат поиска топ-10 новых фильмов по жанру
        :param genre: жанр
        :return: JSON список результата поиска по жанру
        """
        self.cursor.execute(f"""SELECT title, description
                                FROM netflix
                                WHERE listed_in LIKE '%{genre}%' 
                                ORDER BY release_year DESC
                                LIMIT 10
                            """)
        list_result = [
            {'title': _[0],
             'description': _[1]}
            for _ in self.cursor.fetchall()]
        return json.dumps(list_result)

    def get_result_by_cast(self, actor_1, actor_2):
        """
        :param actor_1: актер
        :param actor_2: актер
        :return: список актеров, которые играли с actor_1 и actor_2 больше 2-х раз
        """
        self.cursor.execute(f"""SELECT "cast"
                                FROM netflix
                                WHERE "cast" LIKE '%{actor_1}%' AND "cast" LIKE '%{actor_2}%'""")

        result = self.cursor.fetchall()
        actors_list = []

        for _ in result:
            actors = _[0].split(", ")
            actors_list.extend([actor for actor in actors if actor not in {actor_1, actor_2}])

        actors_counter = Counter(actors_list)
        return [actor for actor, count in actors_counter.items() if count >= 2]

    def get_result_by_filter(self, type_, year, genre):
        """
        :param type_: тип
        :param year: год релиза
        :param genre:  жанр
        :return: список фильмов по фильтру
        """
        self.cursor.execute(f"""SELECT title, description
                                FROM netflix
                                WHERE type = '{type_}' AND release_year = '{year}' AND listed_in LIKE '%{genre}%'""")

        result = self.cursor.fetchall()
        return json.dumps(result)
