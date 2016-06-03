import psycopg2
import csv
import random

conn = psycopg2.connect(database="narvar", user="haven")
cur = conn.cursor()

def main():

    clear_table()

    populate_table(1000, 500, 5)

    get_mean()
    get_stddev()
    get_mean_stddev()

    conn.commit()
    cur.close()
    conn.close()


def populate_table(n, mu, sigma):
    data_to_insert = [random.gauss(mu, sigma) for _ in xrange(n)]
    

    # not really a csv
    with open("rand_num_dist.csv", "wb") as ofile:
        ofile.writelines(["%s,%s\n" % num for num in enumerate(data_to_insert)])
    # internet suggested copy best way to efficiently load db
    with open("rand_num_dist.csv", "r") as ifile:
        cur.copy_from(ifile, 'narvar', sep=",", columns=("id", "num") )

    conn.commit()

def clear_table():
    cur.execute('DROP TABLE IF EXISTS narvar')
    cur.execute('CREATE TABLE narvar (id serial PRIMARY KEY, num decimal);')

    conn.commit()


def get_mean():
    cur.execute("SELECT AVG(num) FROM narvar")
    print cur.fetchone()[0]


def get_stddev():
    cur.execute("SELECT STDDEV(num) FROM narvar")
    print cur.fetchone()[0]

def get_mean_stddev():
    # one call for mean and stddev vs reusing above code
    cur.execute("SELECT AVG(num), STDDEV(num) FROM narvar")
    result = cur.fetchone()
    print result[0], result[1]

if __name__ == '__main__':
    main()
