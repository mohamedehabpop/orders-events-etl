import pandas as pd
import pandas as pd
import datetime
import base64
import pyodbc
from pymongo import MongoClient

# MongoDB Atlas connection string
connection_string = "mongodb+srv://mohamedehabmohamedrashad:<password>@cluster0.jazycer.mongodb.net/?retryWrites=true&w=majority"


# Connect to MongoDB Atlas
client = MongoClient(connection_string)
db = client['bostaevents']
collection = db['bostaevents']

# Retrieve data from the collection
data = list(collection.find())

def encrypt(text):
    # Encode the text using base64
    encoded_bytes = base64.b64encode(text.encode('utf-8'))
    encrypted_text = encoded_bytes.decode('utf-8')
    return encrypted_text

def decrypt(encrypted_text):
    # Decode the encrypted text using base64
    decoded_bytes = base64.b64decode(encrypted_text.encode('utf-8'))
    decrypted_text = decoded_bytes.decode('utf-8')
    return decrypted_text

transformed_data = []
for event in data:
    event['order_time'] = datetime.datetime.strptime(event['order_time'], "%Y-%m-%d %H:%M:%S")
    event['delivery_time'] = datetime.datetime.strptime(event['delivery_time'], "%Y-%m-%d %H:%M:%S")
    # calculating duration
    order_time = event['order_time']
    delivery_time = event['delivery_time']
    duration = delivery_time - order_time
    event['duration'] = duration.total_seconds() / 3600

    # Mask the name
    masked_name = encrypt(event['name'])
    event['name'] = masked_name

    # Mask the mobile number
    masked_mobile_number = encrypt(event['mobile_number'])
    event['mobile_number'] = masked_mobile_number

    # Mask the address
    masked_address = encrypt(event['address'])
    event['address'] = masked_address

    # Mask the email
    masked_email = encrypt(event['email'])
    event['email'] = masked_email

    # Mask the ID
    masked_id = encrypt(str(event['id_number']))
    event['id_number'] = masked_id
    transformed_data.append(event)

df = pd.DataFrame(transformed_data)


# Define the path to your Microsoft Access database file
db_file = r'C:\Users\ALNOUR\Desktop\bosta task\bostadata.accdb'

# Establish a connection to the Microsoft Access database
conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_file

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()


data_columns = ['_id', 'index', 'name', 'mobile_number', 'address', 'email', 'order_number', 'id_number', 'order_time', 'delivery_time', 'tracking_number', 'destination', 'weight', 'carrier', 'shipping_method', 'shipping_cost', 'delivery_status', 'duration']
db_columns = ['_id', 'index', 'name', 'mobile_number', 'address', 'email', 'order_number', 'id_number', 'order_time', 'delivery_time', 'tracking_number', 'destination', 'weight', 'carrier', 'shipping_method', 'shipping_cost', 'delivery_status', 'duration']
for _, row in df[data_columns].iterrows():
    values = tuple(row)
    placeholders = ', '.join(['?'] * len(values))
    cursor.execute(f'''
        INSERT INTO orders ({', '.join(db_columns)})
        VALUES ({placeholders})
    ''', values)
    
conn.commit()


conn.close()