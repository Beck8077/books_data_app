import pandas as pd
import numpy as np
import yaml
import csv
import re
from dateutil import parser
import matplotlib.pyplot as plt
import streamlit as st

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
unique_users = df[['id', 'name', 'address', 'phone', 'email']].drop_duplicates()
unique_users_count = len(unique_users)

# most popular author (by sold book count)
orders_books = df3.merge(df2, left_on='book_id', right_on='id')
orders_books['author_set'] = orders_books['author_set'].apply(lambda x: x[0] if isinstance(x, tuple) else str(x))
author_sales = orders_books.groupby('author_set')['quantity'].sum()
most_popular_author = author_sales.idxmax()  # this is now a string
most_popular_count = author_sales.max()

# top customer by total spending
user_sales = df3.merge(df, left_on='user_id', right_on='id')
spending_per_user = user_sales.groupby('user_id')['paid_price'].sum()
max_spending = spending_per_user.max()
top_customers_ids = spending_per_user[spending_per_user == max_spending].index.tolist()
top_customers_info = df[df['id'].isin(top_customers_ids)]


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
    st.write("User IDs of best buyer(s):", top_customers_ids)
    st.dataframe(top_customers_info)
    st.metric("Top Customer Spending", max_spending)

# -------- TAB 3: Authors --------
with tab3:
    st.header("Number of Unique Author Sets")
    st.metric("Unique Author Sets", unique_author_sets)

    st.header("Most Popular Author(s)")
    st.write(most_popular_author)
    st.write(f"Sold count: {most_popular_count}")