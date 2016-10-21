class TimeStringConverter:
  @staticmethod
  def iso8601(time_to_convert):
    return time_to_convert.replace(second=0,microsecond=0).isoformat()


