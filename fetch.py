from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ElasticsearchWarning
import warnings, boto3, argparse, sys
from datetime import datetime, timedelta

# TODO: 
# [X] Get the period for which to collect data from command line
# [X] Find the timestamp for the start of the given period
# [X] Run the search query in batches
# [X] Handle the exceptions while searching
# [] Add AWS credentials for s3.
# [] Update the bucket name and uploaded data key.

es = None
parser = argparse.ArgumentParser()

def init_parser():
  parser.add_argument("--period", type=str, required=True, help="The time period for which the data you want to fetch.\nUse the following format: NUM_PERIOD\nFor example, 15_days, 7_hours, 10_mins, 2_weeks")
  parser.add_argument("--elastic_host", type=str, required=False)
  parser.add_argument("--index", type=str, required=True)
  return parser.parse_args()

def get_td_object(num, unit):
  if unit == "mins":
    return timedelta(minutes=int(num))
  elif unit == "hours":
    return timedelta(hours=int(num))
  elif unit == "days":
    return timedelta(days=int(num))
  elif unit == "weeks":
    return timedelta(weeks=int(num))

def get_timestamp(period):
  period = period.split('_')
  (num, unit) = (period[0], period[1])
  td_object = get_td_object(num, unit)
  return int((datetime.now() - td_object).timestamp())

def index_document(index, document):
  try:
    es.index(index=index, document=document)
    print(f"Inserted document into index {index}")
  except Exception as e:
    print(f"Error while indexing document\n{e}")
    sys.exit(1)

def index_sample_documents():
  index_document(index=index, document={
    'character': 'Aragon',
    'quote': 'It is not this day.',
    'timestamp': int(datetime.now().timestamp())
  })

  index_document(index=index, document={
    'character': 'Frodo Baggins',
    'quote': 'You are late',
    'timestamp': int(datetime.now().timestamp())
  })

  index_document(index=index, document={
    'character': 'Gandalf',
    'quote': 'A wizard is never late, nor is he early.',
    'timestamp': int((datetime.now()-timedelta(hours=15)).timestamp())
  })

def query_data(index, period_start_timestamp):
  query = {
    'query':{
      'range': {
        'timestamp': {
          'gte': period_start_timestamp,
          'lt': int(datetime.now().timestamp())
        }
      }
    }
  }

  result = []
  try:
    for hit in helpers.scan(es, index=index, query=query):
      result.append(hit["_source"])
  except Exception as e:
    print(f"Error while fetching documents.\n{e}")
    sys.exit()
  return result

# Prevent security warnings
warnings.simplefilter('ignore', ElasticsearchWarning)

# Get the command line args
args = init_parser()

# Connect to elasticsearch cluster
es = Elasticsearch(args.elastic_host or 'http://localhost:9200')
if not es.ping():
  print(f"Error connecting to Elastic search cluster.")
  sys.exit()

# Get index from command line
index = args.index

# index_sample_documents()
# Refresh the index
# es.indices.refresh(index=index)

# Query the index
print(f"Fetching documents from the index {index}")
result = query_data(index=index, period_start_timestamp=get_timestamp(args.period))

# Write the responses to a file
print("Writing data to output file.")
filename = "es_health_" + str(int(datetime.now().timestamp()))
with open(filename, "w") as fp:
  fp.write(str(result))
print("Done!")
# ## Upload to s3

# # Set boto resource to s3
# s3 = boto3.resource("s3")

# # Read data file and upload to s3
# with open("data.txt", "rb") as data:
#     s3.Bucket("BUCKET-NAME").put_object(Key="KEY", body=data)
