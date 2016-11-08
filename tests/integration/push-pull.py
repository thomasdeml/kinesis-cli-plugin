from utils import run_command
import boto3
import sys
import os
import threading

class TestPushPull:

  def __init__(self):
    self.stream_name = 'PushPullTest'
    self.kinesis = boto3.client('kinesis')
    self.pull_succeeded = False

  def setUp(self):
    self.create_stream()

  def tearDown(self):
    self.delete_stream()

  def test_push(self):
    command = 'for i in {1..50};do echo "Hello World $i";sleep 0.1;done | aws kinesis push --disable-batch --push-delay 100 --stream-name ' + self.stream_name
    command_output = os.popen(command).read()
    assert '.....' in command_output
    stream_records = self.get_all_stream_records()
    assert '.....' in stream_records

  def create_stream(self):
    if self.stream_name in self.kinesis.list_streams()['StreamNames']:
      print "Stream {0} already exists. Should be created by test".format(self.stream_name)
      self.kinesis.delete_stream(StreamName=self.stream_name)
      waiter = self.kinesis.get_waiter('stream_not_exists')
      print "Waiting until stream is deleted"
      waiter.wait(
        StreamName= self.stream_name,
        Limit=100,
        ExclusiveStartShardId='string'
      )
    stream = self.kinesis.create_stream(StreamName=self.stream_name, ShardCount=1)
    waiter = self.kinesis.get_waiter('stream_exists')
    print "Waiting until stream exists"
    waiter.wait(
      StreamName= self.stream_name,
      Limit=100,
      ExclusiveStartShardId='string'
    )

  def delete_stream(self):
    print "Deleting stream {0}".format(self.stream_name)
    self.kinesis.delete_stream(StreamName=self.stream_name)

  def get_all_stream_records(self):
    return '.....'
