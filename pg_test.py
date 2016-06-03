import psycopg2
import csv
import random

conn = psycopg2.connect(database="narvar", user="haven")
cur = conn.cursor()

def main():

    cur.execute('DROP TABLE IF EXISTS narvar')

    cur.execute('CREATE TABLE narvar (id serial PRIMARY KEY, num integer);')

    # cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc`def"))

    cur.execute("SELECT * FROM test;")

    print cur.fetchone()

    data_to_insert = populate_table(1000, 500, 5)
    
    # not really a csv
    # internet suggested copy best way to efficiently load db
    
    with open("rand_num_dist.csv", "wb") as ofile:
        ofile.writelines(["%s\n" % num for num in data_to_insert])
    
    with open("rand_num_dist.csv", "r") as ifile:
        cur.copy_from(ifile, 'narvar', columns=("num",) )
    
    conn.commit()
    cur.close()
    conn.close()


def populate_table(n, mu, sigma):
    return [random.gauss(mu, sigma) for _ in xrange(n)]



if __name__ == '__main__':
    main()
