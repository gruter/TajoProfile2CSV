import os
import json
import sqlite3

from metric import *


def usage():
    print('Usage : python3 %s -f <input:json file> <output:csv file>' % (sys.argv[0],))
    sys.exit(0)


def insert_method_data(identified_name, data, ebid):
    for line in data:
        try:
            method_name = line[0].split('.')[1]
        except IndexError:
            if line[0] == 'total' or line[0] == 'fetch':
                method_name = line[0]
            else:
                print('Error!: method name is out of spec: %s', line[0])

        cur.execute('INSERT INTO method_data VALUES (?, ?, ?, null, ?)', (identified_name, method_name, line[1], ebid))


def init_db():
    global conn, cur

    if os.path.isfile('tajo.db'):
        os.remove('tajo.db')

#    conn = sqlite3.connect(':memory:')
    conn = sqlite3.connect('tajo.db')

    cur = conn.cursor()

    cur.execute('CREATE TABLE eb (seq INT, ebid INT)')

    cur.execute('''CREATE TABLE class_data
            (seq INT,
            class TEXT,
            nanotime LONG,
            ebid INT) ''')

    cur.execute(''' CREATE TABLE method_data
            (class TEXT,
            method TEXT,
            nanotime LONG,
            realnano LONG,
            ebid INT) ''')


def calculate_exec_time(ebid):
    # Query for fetching class names in reverse order
    cname_query = 'SELECT class FROM class_data WHERE ebid=? ORDER BY seq DESC'

    # ignore first item which is 'total'
    classes = [x for (x,) in cur.execute(cname_query, (ebid,)).fetchall()[1:]]

    idx = 0
    for id_name in classes:
        class_name = id_name.split('_')[0]
        cdata = get_class_metric_instance(class_name, id_name, ebid)
        print('%s: %d' % (class_name, cdata.calculate_class_totaltime(cur, classes[idx+1:])))
        idx += 1

# main function
if __name__ == '__main__':
    if len(sys.argv) != 4 or sys.argv[1] != '-f':
        usage()

    in_file = open(sys.argv[2])
    out_file = open(sys.argv[3], 'w')

    json_str = in_file.read()

    sidx = json_str.find('<pre>')

    if sidx != -1:
        sidx += 5
        eidx = json_str.find('</pre>')
        json_str = json_str[sidx:eidx]

    json_obj = json.loads(json_str)

#    print(json_str)

    init_db()

    # Data loading
    seq = 1
    for eb_data in json_obj:
        ebid = int(eb_data[0][2:])

        cur.execute('INSERT INTO eb values (?, ?)', (seq, ebid))

        for each_class_data in eb_data[1:]:
            class_name = each_class_data[0][0].split('.')[0]
            cur.execute('INSERT INTO class_data (seq, class, ebid) values (?, ?, ?)', (seq, class_name, ebid))
            seq += 1

            insert_method_data(class_name, each_class_data, ebid)

    conn.commit()

    # Calculation by Excution Block
    ebids = [x for (x,) in cur.execute('SELECT ebid FROM eb').fetchall()]

    for ebid in ebids:
        total_time = cur.execute('SELECT nanotime FROM method_data WHERE class=? AND method=? AND ebid=?'
                                 , ('total', 'total', ebid)).fetchone()[0]

        cur.execute('UPDATE class_data SET nanotime=? WHERE class=? AND ebid=?', (total_time, 'total', ebid))

    total_time = cur.execute('SELECT sum(nanotime) FROM class_data WHERE class=?', ('total',)).fetchone()[0]

    # update total running time(eb id is 0)
    cur.execute('INSERT INTO class_data(class, nanotime) VALUES (?, ?)', ('query_total', total_time))

    # Calculation by Exec
    for ebid in ebids:
        calculate_exec_time(ebid)

    cur.close()
    conn.commit()
    conn.close()
