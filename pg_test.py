import psycopg2
import csv
import random
# import scipy.stats

from flask import Flask, request
import json
from flask.ext.cors import CORS

conn = psycopg2.connect(database="narvar", user="haven")
cur = conn.cursor()

app = Flask(__name__)
CORS(app)

@app.route('/api/populate/<n>/<mu>/<sigma>')
def populate_table(n, mu, sigma):

    #clear table before populating (fixes error with assigning ids below, would fix with more time)
    cur.execute('DROP TABLE IF EXISTS narvar')
    cur.execute('CREATE TABLE narvar (id serial PRIMARY KEY, num decimal);')

    n = int(n)
    mu = int(mu)
    sigma = int(sigma)


    data_to_insert = [random.gauss(mu, sigma) for _ in xrange(n)]
    
    # not really a csv
    with open("rand_num_dist.csv", "wb") as ofile:
        ofile.writelines(["%s,%s\n" % num for num in enumerate(data_to_insert)])
    # internet suggested copy best way to efficiently load db
    with open("rand_num_dist.csv", "r") as ifile:
        cur.copy_from(ifile, 'narvar', sep=",", columns=("id", "num") )

    conn.commit()

    return json.dumps({'success': True}), 200, {'Content-Type': 'application/json; charset=UTF-8'}

@app.route('/api/depopulate')
def clear_table():
    cur.execute('DROP TABLE IF EXISTS narvar')
    cur.execute('CREATE TABLE narvar (id serial PRIMARY KEY, num decimal);')

    conn.commit()

    return json.dumps({'success': True}), 200, {'Content-Type': 'application/json; charset=UTF-8'}


@app.route('/api/mean')
def get_mean():
    cur.execute("SELECT AVG(num) FROM narvar")
    return json.dumps({'mean': float(cur.fetchone()[0])}), 200, {'Content-Type': 'application/json; charset=UTF-8'}


@app.route('/api/stddev')
def get_stddev():
    cur.execute("SELECT STDDEV(num) FROM narvar")
    return json.dumps({'stddev': float(cur.fetchone()[0])}), 200, {'Content-Type': 'application/json; charset=UTF-8'}

@app.route('/api/mean_stddev')
def get_mean_stddev():
    # one call for mean and stddev vs reusing above code
    cur.execute("SELECT AVG(num), STDDEV(num) FROM narvar")
    result = cur.fetchone()
    return json.dumps({'mean': float(result[0]), 'stddev': float(result[1])}), 200, {'Content-Type': 'application/json; charset=UTF-8'}

@app.route('/api/ntile/<number_of_buckets>/<bucket_number>')
def get_ntile(number_of_buckets, bucket_number):
    number_of_buckets = int(number_of_buckets)
    bucket_number = int(bucket_number)
    # this seems like you could just sort the table and then take the num in row (number of rows in table * bucket_number / number_of_buckets)
    cur.execute("SELECT MAX(num), ntile FROM (SELECT num, ntile(%s) OVER (ORDER BY num) AS ntile FROM narvar) AS x WHERE ntile = %s GROUP BY ntile" % (number_of_buckets, bucket_number))
    result = cur.fetchone()
    print result[0]
    return json.dumps({'ntile': float(result[0]), 'number_of_buckets': result[1]}), 200, {'Content-Type': 'application/json; charset=UTF-8'}

@app.route('/api/ntile_array/<number_of_buckets>')
def get_ntile_array(number_of_buckets):
    number_of_buckets = int(number_of_buckets)
    cur.execute("SELECT MAX(num), ntile FROM (SELECT num, ntile(%s) OVER (ORDER BY num) AS ntile FROM narvar) AS x GROUP BY ntile ORDER BY ntile" % (number_of_buckets))
    result = cur.fetchall()
    return_array = []
    for i in result:
        return_array.append(float(i[0]))
    return json.dumps(return_array), 200, {'Content-Type': 'application/json; charset=UTF-8'}


#triaged to make this app into an api mostly due to scipy install time / complications
"""
@app.route('/api/confidence_intervarl/<percent>')
def get_confidence_interval(percent):
    mean, stddev = get_mean_stddev()
    #seems absurd to deal with scipy installation, but quick and dirty
    ci = stats.norm.interval(percent, mean, stddev)

    print ci
"""


if __name__ == '__main__':
    app.run()
