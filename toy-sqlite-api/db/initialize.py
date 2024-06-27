import sqlite3
import random
from datetime import datetime

#randomly generated mock data on companies
companies_data = [
    ("AAPL", "Apple Inc.", random.randint(50000, 150000), random.uniform(130.00, 180.00), int(datetime.now().timestamp())),
    ("MSFT", "Microsoft Corporation", random.randint(50000, 150000), random.uniform(250.00, 350.00), int(datetime.now().timestamp())),
    ("GOOGL", "Alphabet Inc.", random.randint(50000, 150000), random.uniform(120.00, 180.00), int(datetime.now().timestamp())),
    ("AMZN", "Amazon.com Inc.", random.randint(50000, 150000), random.uniform(90.00, 150.00), int(datetime.now().timestamp())),
    ("FB", "Meta Platforms, Inc.", random.randint(50000, 150000), random.uniform(150.00, 300.00), int(datetime.now().timestamp())),
    ("BRK.B", "Berkshire Hathaway Inc.", random.randint(50000, 150000), random.uniform(200.00, 300.00), int(datetime.now().timestamp())),
    ("JNJ", "Johnson & Johnson", random.randint(50000, 150000), random.uniform(130.00, 180.00), int(datetime.now().timestamp())),
    ("V", "Visa Inc.", random.randint(50000, 150000), random.uniform(180.00, 250.00), int(datetime.now().timestamp())),
    ("PG", "Procter & Gamble Co.", random.randint(50000, 150000), random.uniform(120.00, 160.00), int(datetime.now().timestamp())),
    ("TSLA", "Tesla Inc.", random.randint(50000, 150000), random.uniform(100.00, 200.00), int(datetime.now().timestamp()))
]

def initialize_toy_db():
    conn = sqlite3.connect('toy_fi_data.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS company
                    (symbol TEXT PRIMARY KEY, name TEXT, volume INTEGER, price REAL, timestamp INTEGER)''')
    conn.commit()

    cursor.executemany('''
        INSERT OR REPLACE INTO company (symbol, name, volume, price, timestamp) VALUES (?, ?, ?, ?, ?)
    ''', companies_data)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

