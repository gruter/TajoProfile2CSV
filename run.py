import sys
import os
import json
import sqlite3

from metric import *


def usage():
    print('Usage : python3 run.py [-d] <input:json file> [other json files, ...]')
    print('\t-d : for debug (Use local file db. It is made in memory at default.)')
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


def init_db(is_debug):
    global conn, cur

    if is_debug:
        if os.path.isfile('tajo.db'):
            os.remove('tajo.db')
        conn = sqlite3.connect('tajo.db')

    else:
        conn = sqlite3.connect(':memory:')

    cur = conn.cursor()

    cur.execute('CREATE TABLE eb (seq INT, ebid INT)')

    cur.execute('''CREATE TABLE class_data
            (seq INT,
            class TEXT,
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
        cdata = get_class_metric_instance(cur, class_name, id_name, ebid)
        cdata.calculate_class_time(classes[idx+1:])
        idx += 1

    conn.commit()


def print_csv(outf):
    ebids = [x for (x,) in cur.execute('SELECT ebid FROM eb ORDER BY seq ASC').fetchall()]

    class_names = []

    total_time = cur.execute('SELECT nanotime FROM method_data WHERE class=?', ('query_total',)).fetchone()[0]

    for ebid in ebids:
        classes = [x for (x,) in cur.execute('SELECT class FROM class_data WHERE ebid = ? ORDER BY seq ASC',
                                             (ebid,)).fetchall()]
        for cname in classes:
            class_names.append((ebid, cname))

        ebtime = cur.execute('SELECT nanotime FROM method_data WHERE ebid=? AND class=? AND method=?',
                             (ebid, 'total', 'total')).fetchone()[0]

        outf.write('EB%d,,,%d,%f\n' % (ebid, ebtime, ebtime/total_time))

        for cname in classes:
            methods = cur.execute('SELECT class, method, nanotime, realnano FROM method_data WHERE class=? AND ebid=?',
                                  (cname, ebid)).fetchall()

            if methods[0][0] == 'total':
                continue

            for each_method in methods:
                ''' index 0 : class
                    1 : method
                    2 : nanotime
                    3 : realnano '''
                if each_method[2] == 0 or each_method[1] == 'inTuples' or each_method[1] == 'outTuples':
                    continue

                nanotime = each_method[2] if each_method[3] is None else each_method[3]
                outf.write(',%s,%s,%d,%.5f\n' % (each_method[0], each_method[1], nanotime, nanotime / ebtime))

        outf.write('\n')


def real_main(inf_name, outf_name, is_debug):
    in_file = open(inf_name)
    out_file = open(outf_name, 'w')

    json_str = in_file.read()
    in_file.close()

    sidx = json_str.find('<pre>')

    if sidx != -1:
        sidx += 5
        eidx = json_str.find('</pre>')
        json_str = json_str[sidx:eidx]

    json_obj = json.loads(json_str)

    init_db(is_debug)

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

    total_time = cur.execute('SELECT sum(nanotime) FROM method_data WHERE class=? AND method=?'
                             , ('total', 'total')).fetchone()[0]

    # update total running time
    cur.execute('INSERT INTO method_data(class, nanotime) VALUES (?, ?)', ('query_total', total_time))

    # Calculation by Exec
    for ebid in ebids:
        calculate_exec_time(ebid)

    print_csv(out_file)
    out_file.close()

    cur.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        usage()
        sys.exit(0)

    is_debug = False
    args = sys.argv[1:]

    if sys.argv[1] == '-d':
        is_debug = True
        args = sys.argv[2:]

    for infile_name in args:
        dotidx = infile_name.rfind('.')

        if dotidx == -1:
            outfile_name = infile_name+'.csv'
        else:
            outfile_name = infile_name[:dotidx]+'.csv'

        real_main(infile_name, outfile_name, is_debug)
