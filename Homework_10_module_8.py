import pika
from faker import Faker
from pymongo import MongoClient
from bson.objectid import ObjectId

def connect_to_mongodb():
    client = MongoClient("mongodb+srv://<db_username>:<db_password>@hwcluster.atmq6.mongodb.net/?retryWrites=true&w=majority&appName=HWcluster")
    db = client["db_contacts"]
    return db

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='email_queue')
    return connection, channel

def generate_contacts(db, n=10):
    faker = Faker()
    contacts_collection = db['contacts']
    contacts = []
    for _ in range(n):
        contact = {
            'fullname': faker.name(),
            'email': faker.email(),
            'sent': False,
            'phone_number': faker.phone_number(),
            'address': faker.address()
        }
        contact_id = contacts_collection.insert_one(contact).inserted_id
        contacts.append(contact_id)
    return contacts

def producer(n=10):
    db = connect_to_mongodb()
    connection, channel = connect_to_rabbitmq()
    print("RabbitMQ connected as Producer.")

    contacts = generate_contacts(db, n)
    print(f"Generated {len(contacts)} contacts.")

    for contact_id in contacts:
        message = str(contact_id)
        channel.basic_publish(exchange='', routing_key='email_queue', body=message)
        print(f"Placed in queue: Contact ID {contact_id}")

    connection.close()
    print("Producer finished.")

def send_email(contact):
    print(f"Sending email to: {contact['fullname']} ({contact['email']})...")
    print("Email sent!")

def callback(ch, method, properties, body):
    db = connect_to_mongodb()
    contacts_collection = db['contacts']
    contact_id = body.decode('utf-8')
    contact = contacts_collection.find_one({"_id": ObjectId(contact_id)})
    
    if contact and not contact['sent']:
        send_email(contact)
        contacts_collection.update_one({"_id": ObjectId(contact_id)}, {"$set": {"sent": True}})
        print(f"Updated status for {contact['fullname']}")
    else:
        print(f"Contact with ID {contact_id} not found or already sent.")

def consumer():
    connection, channel = connect_to_rabbitmq()
    print("RabbitMQ connected as Consumer.")
    channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages...')
    channel.start_consuming()

def main():
    mode = input("Enter mode (producer/consumer): ").strip().lower()
    if mode == 'producer':
        num_contacts = int(input("How many contacts to generate? "))
        producer(num_contacts)
    elif mode == 'consumer':
        consumer()
    else:
        print("Invalid mode. Please enter 'producer' or 'consumer'.")

if __name__ == '__main__':
    main()
