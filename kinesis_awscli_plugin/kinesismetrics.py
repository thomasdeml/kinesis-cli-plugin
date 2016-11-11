import logging
import sys


class KinesisMetrics(object):
    def __init__(self, metric_id, datapoints, statistic):
        self.datapoints = datapoints
        self.metric_id = metric_id
        self.statistic = statistic
        if len(datapoints) > 0:
            self._has_data = True
        else:
            self._has_data = False

    @property
    def datapoint_average(self):
        # avoid division by zero
        if len(self.metric_values()) > 0:
            return sum(self.metric_values()) / len(self.metric_values())
        else:
            return 0

    @property
    def datapoint_max(self):
        return max(self.metric_values())

    @property
    def datapoint_min(self):
        return min(self.metric_values())

    @property
    def has_data(self):
        return self._has_data

    def metric_values(self):
        return map(lambda x: float(x[self.statistic]), self.datapoints)
