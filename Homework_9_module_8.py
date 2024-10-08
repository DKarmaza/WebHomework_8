from pymongo import MongoClient
import json

def db_connect():
    client = MongoClient(
    "mongodb+srv://<db_username>:<db_password>@hwcluster.atmq6.mongodb.net/?retryWrites=true&w=majority&appName=HWcluster")
    db = client["authors_quotes"]
    return db

def load_authors(db):
    with open('authors.json', 'r', encoding='utf-8') as f:
        authors_data = json.load(f)
        authors_collection = db.authors
        for author_data in authors_data:
            if not authors_collection.find_one({"fullname": author_data['fullname']}):  # Перевіряємо, чи існує вже автор
                authors_collection.insert_one(author_data)

def load_quotes(db):
    with open('qoutes.json', 'r', encoding='utf-8') as f:
        quotes_data = json.load(f)
        authors_collection = db.authors
        quotes_collection = db.quotes
        for quote_data in quotes_data:
            author = authors_collection.find_one({"fullname": quote_data['author']})
            if author:
                quote_data['author_id'] = author['_id']
                quotes_collection.insert_one(quote_data)

def search_by_tag(db, tag):
    quotes_collection = db.quotes
    quotes = quotes_collection.find({"tags": tag})
    for quote in quotes:
        author = db.authors.find_one({"_id": quote["author_id"]})
        print(f'{quote["quote"]} - {author["fullname"]}')

def search_by_tags(db, tags):
    quotes_collection = db.quotes
    tag_list = tags.split(',')
    quotes = quotes_collection.find({"tags": {"$in": tag_list}})
    for quote in quotes:
        author = db.authors.find_one({"_id": quote["author_id"]})
        print(f'{quote["quote"]} - {author["fullname"]}')

def search_by_author(db, name):
    authors_collection = db.authors
    quotes_collection = db.quotes
    author = authors_collection.find_one({"fullname": name})
    if author:
        quotes = quotes_collection.find({"author_id": author['_id']})
        for quote in quotes:
            print(f'{quote["quote"]} - {name}')
    else:
        print(f'Author "{name}" not found')

def main():
    db = db_connect()
    print("DB connected")

    load_authors(db)
    load_quotes(db)
    print("Data loaded")

    while True:
        command = input("Enter your command (e.g., name: Albert Einstein, tag:life, tags:life,miracles, exit to quit): ")
        if command.startswith("name:"):
            name = command.split("name:")[1].strip()
            search_by_author(db, name)
        elif command.startswith("tag:"):
            tag = command.split("tag:")[1].strip()
            search_by_tag(db, tag)
        elif command.startswith("tags:"):
            tag = command.split("tags:")[1].strip()
            search_by_tags(db, tags)
        elif command == "exit":
            print("Exiting in progress")
            break
        else:
            print("Invalid command")

if __name__ == "__main__":
    main()