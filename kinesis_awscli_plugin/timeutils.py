import datetime

class TimeUtils:
  @staticmethod
  def iso8601(time_to_convert):
    return time_to_convert.replace(second=0,microsecond=0).isoformat()

  @staticmethod
  def dateparser_available():
    try: 
      import dateparser
      return True
    except ImportError,NameError:
      return False

  @staticmethod
  def to_datetime(time_string):
    if TimeUtils.dateparser_available() == True:
       import dateparser
       return dateparser.parse(time_string, settings={'TIMEZONE': 'UTC'})
    else: 
       return datetime.datetime.strptime( time_string, "%Y-%m-%dT%H:%M:%SZ")
