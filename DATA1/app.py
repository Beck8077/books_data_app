import pandas as pd
import numpy as np
import yaml
import csv
import re
from sqlalchemy import create_engine, text
from dateutil import parser
import os
import matplotlib.pyplot as plt
import streamlit as st

engine = create_engine('postgresql+psycopg2://postgres:bekzod8077@localhost:5432/postgres')

# --------------------- Cleaning data ---------------------

# ================  USERS   ================

df = pd.read_csv('users.csv').drop_duplicates()
df['phone'] = df['phone'].astype(str).str.replace(r'\D', '', regex=True)
df['phone'] = df['phone'].apply(lambda x: x[:10].ljust(10, '0'))     # fix short numbers safely
df['phone'] = df['phone'].apply(lambda x: x[0:3] + '-' + x[3:6] + '-' + x[6:10])
df = df.fillna('No Info')


# ================  BOOKS   ================

with open('books.yaml', 'r', encoding='utf-8') as read_file:
    books = yaml.safe_load(read_file)

clean_books = [{k.lstrip(':'): v for k, v in b.items()} for b in books]

df2 = pd.DataFrame(clean_books).drop_duplicates()
df2 = df2.replace('NULL', 'No Info').fillna('No Info')

def clean_year(x):
    try:
        s = str(x).strip()
        if s.isdigit():
            return int(s)
        return None
    except:
        return None
df2["year"] = df2["year"].apply(clean_year)


# ================  ORDERS  ================

df3 = pd.read_parquet('orders.parquet').drop_duplicates()
df3 = df3.fillna('No Info').replace(['NULL', ''], 'No Info')

# cleaning and changing formats in timetable column
def clean_timestamp(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()

    s = s.replace(";", " ").replace(",", " ").replace("  ", " ")
    s = re.sub(r'\bA\.?M\.?\b', 'AM', s, flags=re.IGNORECASE)
    s = re.sub(r'\bP\.?M\.?\b', 'PM', s, flags=re.IGNORECASE)
    match = re.match(r'^(\d{1,2}:\d{2}(:\d{2})?)(.*?)(\d{1,4}[-/][A-Za-z0-9]+[-/]\d{2,4})$', s)
    if match:
        s = f"{match.group(4)} {match.group(1)}"
    try:
        return parser.parse(s, dayfirst=False)
    except:
        try:
            return parser.parse(s, dayfirst=True)
        except:
            return pd.NaT

df3['timestamp'] = df3['timestamp'].apply(clean_timestamp)
df3['timestamp'] = pd.to_datetime(df3['timestamp'], errors='coerce')

df3['date_only'] = df3['timestamp'].dt.strftime('%Y-%m-%d')

conversion_rates = {
    'USD': 1,
    '$': 1,
    'EUR': 1.2,
    '€': 1.2
}

# currency converting
def convert_to_usd(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    currency = 'USD'
    if '€' in x or 'EUR' in x.upper():
        currency = 'EUR'
    elif '$' in x or 'USD' in x.upper():
        currency = 'USD'

    x = re.sub(r'[^\d.,¢]', '', x)
    x = x.replace('¢', '.')
    x = re.sub(r'\.(?=.*\.)', '', x)
    x = x.replace(',', '.')

    try:
        amount = float(x)
        return round(amount * conversion_rates[currency], 2)
    except:
        return None

df3['unit_price'] = df3['unit_price'].apply(convert_to_usd)
df3['paid_price'] = df3['quantity'] * df3['unit_price']

# connection to postgres
if not os.getenv("STREAMLIT_ENV"):  # Running locally, not in Streamlit Cloud
    engine = create_engine('postgresql+psycopg2://postgres:bekzod8077@localhost:5432/postgres')

    with engine.connect() as db:
        db.execute(text("DROP TABLE IF EXISTS users;"))
        db.commit()
    df.to_sql('users', engine, index=False)

    with engine.connect() as db2:
        db2.execute(text("DROP TABLE IF EXISTS books;"))
        db2.commit()
    df2.to_sql('books', engine, index=False)

    with engine.connect() as db3:
        db3.execute(text("DROP TABLE IF EXISTS orders;"))
        db3.commit()
    df3.to_sql('orders', engine, index=False)


# ----------------------------------------------------

# daily revenue and top 5 days
sum_days = (df3.groupby("date_only", as_index=False)["paid_price"].sum())
top_5_days = sum_days.nlargest(5, "paid_price").sort_values("date_only")
print(top_5_days)

# unique sets of authors
def normalize_authors(a):
    return tuple(sorted([x.strip() for x in a.split(',')]))
df2['author_set'] = df2['author'].apply(normalize_authors)
unique_author_sets = df2['author_set'].nunique()

# real unique users
def normalize_user(row):
    return (row['name'], row['address'], row['phone'], row['email'])
df['user_key'] = df.apply(normalize_user, axis=1)
unique_users_count = df['user_key'].nunique()

# most popular author (by sold book count)
orders_books = df3.merge(df2, left_on='book_id', right_on='id')
author_sales = orders_books.groupby('author_set')['quantity'].sum()
most_popular_authors = author_sales.idxmax()
most_popular_count = author_sales.max()

# top customer by total spending
user_sales = df3.merge(df, left_on='user_id', right_on='id')
user_spending = user_sales.groupby('user_key')['paid_price'].sum()
top_customer = [list(user) for user, val in user_spending.items() if val == user_spending.max()]


plt.figure(figsize=(12,6))
plt.plot(top_5_days['date_only'], top_5_days['paid_price'], marker='o', linestyle='-')
plt.title('Daily Revenue Over Time')
plt.xlabel('Date')
plt.ylabel('Revenue')
plt.grid(True)
plt.show()


# ------------------------- Uploading to Server -------------------------

st.set_page_config(page_title="Book Store Analytics", layout="wide")
st.title("📊 Book Store Analytics Dashboard")

tab1, tab2, tab3 = st.tabs(["📅 Revenue", "👥 Users", "📚 Authors"])

# -------- TAB 1: Revenue --------
with tab1:
    st.header("Top 5 Days by Revenue")
    st.dataframe(top_5_days)

    st.header("Daily Revenue Chart")
    st.line_chart(sum_days.set_index("date_only")["paid_price"])

# -------- TAB 2: Users --------
with tab2:
    st.header("Number of Unique Users")
    st.metric("Unique Users", unique_users_count)

    st.header("Top Customer(s)")
    st.write(top_customer)

# -------- TAB 3: Authors --------
with tab3:
    st.header("Number of Unique Author Sets")
    st.metric("Unique Author Sets", unique_author_sets)

    st.header("Most Popular Author(s)")
    st.write(most_popular_authors)
    st.write(f"Sold count: {most_popular_count}")