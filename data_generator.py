import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker

# Initialize Faker instance
fake = Faker()
index = 'main' 
# Generate random logistics data
def generate_random_logistics_data():
    name = fake.name()
    mobile_number = fake.phone_number()
    address = fake.address()
    email = fake.email()
    order_number = fake.random_number(digits=6)
    id_number = fake.random_number(digits=8)
    order_time = fake.date_time_this_month().strftime('%Y-%m-%d %H:%M:%S')
    delivery_time =  fake.date_time_between(start_date='+1d', end_date='+7d').strftime('%Y-%m-%d %H:%M:%S')
    tracking_number = fake.random_number(digits=10)
    destination = fake.city()
    weight = round(random.uniform(0.1, 50.0), 2)
    carrier = fake.company()
    shipping_method = random.choice(['air', 'sea', 'ground'])
    shipping_cost = round(random.uniform(10.0, 100.0), 2)
    delivery_status = random.choice(['in transit', 'delivered', 'pending'])
    
    data = {
        'index': index,
        'name': name,
        'mobile_number': mobile_number,
        'address': address,
        'email': email,
        'order_number': order_number,
        'id_number': id_number,
        'order_time': order_time,
        'delivery_time': delivery_time,
        'tracking_number': tracking_number,
        'destination': destination,
        'weight': weight,
        'carrier': carrier,
        'shipping_method': shipping_method,
        'shipping_cost': shipping_cost,
        'delivery_status': delivery_status,
    }
    
    return data
generate_random_logistics_data()

# MongoDB Atlas connection string
connection_string = "mongodb+srv://mohamedehabmohamedrashad:<password>@cluster0.jazycer.mongodb.net/?retryWrites=true&w=majority"
# Connect to MongoDB Atlas
client = MongoClient(connection_string)

# Access the desired database
db = client['bostaevents']

# Access the desired collection
collection = db['bostaevents']

# # Generate and insert random logistics data
for _ in range(500):
    data = generate_random_logistics_data()
    collection.insert_one(data)

client.close()