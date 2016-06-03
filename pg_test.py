import psycopg2
import csv
import random

conn = psycopg2.connect(database="narvar", user="haven")
cur = conn.cursor()

def main():

    clear_table()

    cur.execute("SELECT count(*) FROM narvar;")
    print cur.fetchone()[0]

    populate_table(1000, 500, 5)
        
    cur.execute("SELECT count(*) FROM narvar;")
    print cur.fetchone()[0]

    clear_table()

    cur.execute("SELECT count(*) FROM narvar;")
    print cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()


def populate_table(n, mu, sigma):
    data_to_insert = [random.gauss(mu, sigma) for _ in xrange(n)]
    

    # not really a csv
    with open("rand_num_dist.csv", "wb") as ofile:
        ofile.writelines(["%s,%s,\n" % num for num in enumerate(data_to_insert)])
    # internet suggested copy best way to efficiently load db
    with open("rand_num_dist.csv", "r") as ifile:
        cur.copy_from(ifile, 'narvar', sep=",", columns=("id", "num") )

    conn.commit()

def clear_table():
    cur.execute('DROP TABLE IF EXISTS narvar')
    cur.execute('CREATE TABLE narvar (id serial PRIMARY KEY, num decimal);')

    conn.commit()


if __name__ == '__main__':
    main()
