import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Book, Shop, Stock, Sale

DSN = 'postgresql://postgres:postgres@localhost:5432/Netology_DB'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind = engine)
session = Session()


with open("DB_filling.json", "r") as file:
    data = json.load(file)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


search = Publisher.name == input("Введите название издателя: ")
querry = session.query(Book.title, Shop.name, Sale.price, Sale.count, Sale.date_sale).\
    join(Publisher).join(Stock).join(Shop).join(Sale).filter(search).order_by(Sale.date_sale)

for book, shop, price, count, date in querry:
    print (f" Книга: {book} | Магазин: {shop} | Цена: {price} | Количество: {count} | Дата: {date}")
    
session.close()