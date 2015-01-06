QUERY_GET_METHOD_NANOTIME = 'SELECT nanotime FROM method_data WHERE class=? AND ebid=? AND method=?'


class BaseClassMetric:
    def __init__(self, cur, name, ebid):
        self.name = name
        self.ebid = ebid
        self.cur = cur

    # get 'nanotime'
    def get_method_time(self, **kwargs):
        return self.cur.execute(QUERY_GET_METHOD_NANOTIME,
                                (kwargs['cname'], self.ebid, kwargs['method'])).fetchone()[0]

    # update 'realtime'
    def update_method_time(self, method, time):
        self.cur.execute('UPDATE method_data SET realnano=? WHERE ebid=? AND class=? AND method=?',
                         (time, self.ebid, self.name, method))

    def calculate_class_time(self, prev_classes):
        try:
            prev_class = prev_classes[0]

            prev_next_time = self.get_method_time(cname = prev_class, method='next')
            curr_class_next_time = self.get_method_time(cname=self.name, method='next')

            self.update_method_time('next', curr_class_next_time - prev_next_time)

        except IndexError:
            pass

        return 0


class HashJoinExecMetric(BaseClassMetric):
    def calculate_class_time(self, prev_classes):
        seqscans = []

        for cname in prev_classes:
            if cname.startswith('SeqScanExec'):
                seqscans.append(cname)

        seqscan_sum = sum([self.get_method_time(cname=x, method='next') for x in seqscans])

        curr_class_next_time = self.get_method_time(cname=self.name, method='next')

        self.update_method_time('next', curr_class_next_time - seqscan_sum)


class SeqScanExecMetric(BaseClassMetric):
    def calculate_class_time(self, prev_classes):
        next_time = self.get_method_time(cname=self.name, method='next')

        try:
            noteval_time = self.get_method_time(cname=self.name, method='NotEval')
        except TypeError:
            noteval_time = 0

        try:
            bineval_time = self.get_method_time(cname=self.name, method='BnaryEval')
        except TypeError:
            bineval_time = 0

        project_time = self.get_method_time(cname=self.name, method='project')

        self.update_method_time('next', next_time - noteval_time - bineval_time - project_time)


class ExternalSortExecMetric(BaseClassMetric):
    def calculate_class_time(self, prev_classes):
        prev_class = prev_classes[0]

        prev_next_time = self.get_method_time(cname=prev_class, method='next')
        sort_write_total_time = self.get_method_time(cname=self.name, method='SortWrite')
        sort_scan_total_time = self.get_method_time(cname=self.name, method='SortScan')
        sort_total_time = self.get_method_time(cname=self.name, method='Sort')

        self.update_method_time('Sort', sort_total_time - sort_write_total_time - sort_scan_total_time - prev_next_time)

        next_total_time = self.get_method_time(cname=self.name, method='next')
        self.update_method_time('next', next_total_time - sort_total_time)


class HashAggregateExecMetric(BaseClassMetric):
    def calculate_class_time(self, prev_classes):
        prev_class = prev_classes[0]

        prev_next_time = self.get_method_time(cname=prev_class, method='next')

        compute_time = self.get_method_time(cname=self.name, method='compute')
        next_time = self.get_method_time(cname=self.name, method='next')

        self.update_method_time('compute', compute_time - prev_next_time)
        self.update_method_time('next', next_time - compute_time)

class_name_map = {
    'HashAggregateExec': HashAggregateExecMetric,
    'HashJoinExec': HashJoinExecMetric,
    'SeqScanExec': SeqScanExecMetric,
    'ExternalSortExec': ExternalSortExecMetric
}


def get_class_metric_instance(cur, class_name, id_name, ebid):
    try:
        inst = class_name_map[class_name]
    except KeyError:
        inst = BaseClassMetric

    return inst(cur, id_name, ebid)
