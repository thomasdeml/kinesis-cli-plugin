from kinesis_awscli_plugin.lib.timeutils import TimeUtils

class TestTimeUtils:
  def test_dateparser_available(self):
     # dateparser_available should return True or False and not throw exception
     is_available = TimeUtils.dateparser_available()
     assert (is_available == True or is_available == False)

