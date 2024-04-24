from google.colab import auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token

auth.authenticate_user()

from faker import Faker
import random
import time
import string
from google.cloud import pubsub_v1

# Create Faker instance
fake = Faker()
# Open CSV file in write mode


# Demo code variables
project_id = "project-sudheer-421110"
topic_name = "taskk"

# Create a PublisherClient
publisher = pubsub_v1.PublisherClient()

# Create the topic path
topic_path = publisher.topic_path(project_id, topic_name)



for _ in range(10):
  lis=[]
  name = fake.name()
  age = random.choice([28, 29, 30])
  gender = random.choice(["Male", "Female"])
  course = random.choice(["DB", "Cloud", "PF", "DSA", "MVC", "OOP"])
  marks = random.randint(0, 100)
  email = fake.email()
  lis.append(name)
  lis.append(age)
  lis.append(gender)
  lis.append(course)
  lis.append(marks)
  lis.append(email)
  message_data_bytes = str(lis).encode("utf-8")
  future = publisher.publish(topic_path, data=message_data_bytes)
  future.result()  # Wait for the message to be published
  print(lis)
  time.sleep(10)

!pip install google.auth
from google.colab import auth
auth.authenticate_user()
keyfile_path = "/content/project-sudheer-421110-93b1de29218c.json"
!gcloud auth activate-service-account --key-file="$keyfile_path"

from google.cloud import pubsub_v1
from google.cloud import storage
import csv
import os

# Function to process messages
def process_message(message):
    # Assuming the message is in the format [name, email, age]
    data = message.data.decode('utf-8')
    elements = data.strip("[]").split(",")  # Strip square brackets and split by comma
    elements = [element.strip().strip("'\"") for element in elements]

    # Creating a dictionary with keys 'name', 'email', and 'age'
    json_data = {
        'name': elements[0],
        'age': elements[1],
        'gender': elements[2],
        'course': elements[3],
        'marks': elements[4],
        'email': elements[5],
    }
    return json_data

# Set your project ID, topic name, subscription name, and bucket name
project_id = "project-sudheer-421110"
topic_name = "taskk"
subscription_name = "taskk-sub"
bucket_name = "taskk1"

# Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Create a storage client
storage_client = storage.Client()

# Define the topic path
topic_path = f"projects/{project_id}/topics/{topic_name}"

# Check if the subscription exists, if not, create it
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"

# subscription = subscriber.create_subscription(subscription_path, topic_path)

# Pull messages
response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 5})

# Process messages and write to JSON file
json_data_list = []
for msg in response.received_messages:
    json_data = process_message(msg.message)
    print(json_data)
    json_data_list.append(json_data)
    print(json_data_list)

    # Acknowledge the message
    subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": [msg.ack_id]})

# Write JSON data to a temporary file
temp_file_path = "/tmp/sample.json"  # Path to a temporary file
with open(temp_file_path, 'w') as json_file:
    json.dump(json_data_list, json_file)
# ... (previous code remains unchanged)


csv_file_path = "/tmp/sample1.csv"
with open(csv_file_path, 'w', newline='') as csv_file:
    fieldnames = ['name', 'age', 'gender', 'course', 'marks', 'email']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(json_data_list)

    for msg in response.received_messages:
        elements = process_message(msg.message)
        writer.writerow(dict(zip(fieldnames, elements)))

        # Acknowledge the message
        subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": [msg.ack_id]})

# Upload the CSV file to the bucket
csv_blob = bucket.blob('sample1.csv')
csv_blob.upload_from_filename(csv_file_path)

# Clean up the temporary CSV file
os.remove(csv_file_path)

print("Data uploaded successfully!")

# ... (rest of your code remains unchanged)

# Upload the JSON file to the bucket
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob('sample.json')  # Name of the JSON file in the bucket
blob.upload_from_filename(temp_file_path)

# Delete the temporary file
os.remove(temp_file_path)

# Create a text file with the same content
text_content = "\n".join(f"{field}: {value}" for field, value in zip(fieldnames, elements))
text_file_path = "/tmp/sample.txt"
with open(text_file_path, 'w') as text_file:
    text_file.write(text_content)

# Upload the text file to the bucket
text_blob = bucket.blob('sample.txt')
text_blob.upload_from_filename(text_file_path)

# Clean up temporary files
os.remove(text_file_path)



