
class ExecutionBlock:
    # Execution Block DAO

    def __init__(self, ebid, cur):
        self.ebid = ebid
        self.cursor = cur
        self.metrics = []

    def add_metric(self, metric):
        self.metrics.append(metric)
