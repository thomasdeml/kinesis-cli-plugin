require 'aws-sdk'

STREAM_NAME = 'MetricsTest'
REGION = 'us-east-1'

def main
  kinesis = Aws::Kinesis::Client.new(region: REGION)
  1.upto(200) do
    records = records_to_put(50)
    kinesis.put_records(
      stream_name: STREAM_NAME,
      records: records
    )
    puts "Put #{records.size} records"
    sleep 0.2
  end
end

def records_to_put(record_count, record_size=1000)
  records = []
  1.upto(record_count) do |i|
     records << {
        partition_key: i.to_s,
        data: 'y' * record_size
     }
  end
  return records
end

main
