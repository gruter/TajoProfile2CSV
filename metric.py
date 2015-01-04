import sys


QUERY_GET_METHOD_NANOTIME = 'SELECT nanotime FROM method_data WHERE class=? AND ebid=? AND method=?'

class BaseClassMetric:
    def __init__(self, name, ebid):
        self.name = name
        self.ebid = ebid

    def calculate_class_totaltime(self, cur, prev_classes):
        try:
            prev_class = prev_classes[0]

            prev_next_time = cur.execute(QUERY_GET_METHOD_NANOTIME, (prev_class, self.ebid, 'next')).fetchone()[0]

            curr_class_next_time = cur.execute(QUERY_GET_METHOD_NANOTIME, (self.name, self.ebid, 'next')).fetchone()[0]

            return curr_class_next_time - prev_next_time

        except IndexError:
            pass

        return 0


class HashJoinExecMetric(BaseClassMetric):
    def calculate_class_totaltime(self, cur, prev_classes):
        seqscans = []

        for cname in prev_classes:
            if cname.startswith('SeqScanExec'):
                seqscans.append(cname)

        seqscan_sum = sum([cur.execute(QUERY_GET_METHOD_NANOTIME, (x, self.ebid, 'next')).fetchone()[0]
                           for x in seqscans])

        curr_class_next_time = cur.execute(QUERY_GET_METHOD_NANOTIME, (self.name, self.ebid, 'next')).fetchone()[0]

        return curr_class_next_time - seqscan_sum


class_name_map = {
    'HashJoinExec': HashJoinExecMetric
}


def get_class_metric_instance(class_name, id_name, ebid):
    try:
        inst = class_name_map[class_name]
    except KeyError:
        inst = BaseClassMetric

    return inst(id_name, ebid)
