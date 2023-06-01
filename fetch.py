from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ElasticsearchWarning
import warnings

# Prevent security warnings
warnings.simplefilter('ignore', ElasticsearchWarning)

# Connect to elasticsearch cluster
es = Elasticsearch('http://localhost:9200')

# Create new index if doesn't exist
index = 'lord-of-the-rings-2'
if not es.indices.exists(index=index):
    es.indices.create(index=index)
    print(f"Index {index} created successfully.")

# Index a few documents to the index
es.index(
 index=index,
 document={
  'character': 'Aragon',
  'quote': 'It is not this day.'
 })

print(f"Inserted document into index {index}")

es.index(
 index=index,
 document={
  'character': 'Gandalf',
  'quote': 'A wizard is never late, nor is he early.'
 })
print(f"Inserted document into index {index}")


es.index(
 index=index,
 document={
  'character': 'Frodo Baggins',
  'quote': 'You are late'
 })
print(f"Inserted document into index {index}")


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

# Print the responses
print(result['hits']['hits'])