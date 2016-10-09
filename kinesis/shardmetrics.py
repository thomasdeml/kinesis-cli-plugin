class ShardMetrics(object):

  def __init__(self, shard_id, metric_values):
    self.metric_values = metric_values
    self.shard_id = shard_id
    if len(metric_values) > 0:
      self._has_data = True
    else:
      self._has_data = False

  def avg(self):
    # avoid division by zero
    if self.metric_values: 
      return (sum(self.metric_values) / len(self.metric_values))
    else:  
      return 0

  def max(self):
    return max(self.metric_values)

  def min(self):
    return min(self.metric_values)


  def has_data(self):
    return self._has_data
