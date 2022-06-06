from flask import Flask

from dao.netflix_dao import NetflixDAO

app = Flask(__name__)

#отключаем сортировку
app.config['JSON_SORT_KEYS'] = False

db = NetflixDAO('netflix.db')


@app.route('/')
def main():
    return 'Главная'


@app.route('/movie/<title>')
def movie_by_title(title):
    return db.get_result_by_title(title)


@app.route('/movie/<int:year_1>/to/<int:year_2>')
def movie_by_year(year_1, year_2):
    return db.get_result_by_years(year_1, year_2)


@app.route('/rating/<group>')
def movie_by_rating(group):
    return db.get_result_by_rating(group)


@app.route('/genre/<genre>')
def movie_by_genre(genre):
    return db.get_result_by_genre(genre)


if __name__ == '__main__':
    app.run()
