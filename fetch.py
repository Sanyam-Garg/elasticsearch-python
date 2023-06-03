from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
import warnings, boto3

# TODO: 
# 1. Add AWS credentials for s3.
# 2. Update the bucket name and uploaded data key.

es = None

def index_document(index, document):
  try:
    es.index(index=index, document=document)
    print(f"Inserted document into index {index}")
  except:
    print("Error while indexing document.")
    exit(1)


# Prevent security warnings
warnings.simplefilter('ignore', ElasticsearchWarning)

# Connect to elasticsearch cluster
try:
  es = Elasticsearch('http://localhost:9200')
except:
  print("Error connecting to Elastic search cluster.")
  exit(1)

# Create new index if doesn't exist
index = 'lord-of-the-rings-2'
if not es.indices.exists(index=index):
    try:
      es.indices.create(index=index)
      print(f"Index {index} created successfully.")
    except:
      print(f"Error occurred while creating index {index}.")

# Index a few documents to the index
index_document(index=index, document={
  'character': 'Aragon',
  'quote': 'It is not this day.'
 })

index_document(index=index, document={
  'character': 'Frodo Baggins',
  'quote': 'You are late'
 })

index_document(index=index, document={
  'character': 'Gandalf',
  'quote': 'A wizard is never late, nor is he early.'
 })


# Refresh the index
es.indices.refresh(index=index)

# Query the index
print(f"Fetching documents from the index {index}")
result = es.search(
 index=index,
  query={
    'match': {'quote': 'late'}
  }
 )

# Write the responses to a file
with open("data.txt", "w") as fp:
  fp.write(str(result['hits']['hits']))

# ## Upload to s3

# # Set boto resource to s3
# s3 = boto3.resource("s3")

# # Read data file and upload to s3
# with open("data.txt", "rb") as data:
#     s3.Bucket("BUCKET-NAME").put_object(Key="KEY", body=data)
