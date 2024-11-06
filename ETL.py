# %%
# # Python Extract Transform Load Example
import requests  # to pull data from sources using APIs - extract
import pandas as pd  # for tabular type of data like .csv, excel, etc. –
import sqlite3
# transformation
from sqlalchemy import create_engine
# %%
def extract() -> dict:
    # This API extracts data from http://universities.hipolabs.com
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data

# %%
def transform(data: dict) -> pd.DataFrame:
    """Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in California {len(df)}")
    df["domains"] = [",".join(map(str, l)) for l in df["domains"]]
    df["web_pages"] = [",".join(map(str, l)) for l in df["web_pages"]]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "web_pages", "name"]]

# %%
def load(df: pd.DataFrame) -> None:
    """Loads data into an SQLite database using sqlite3 connection."""
    # Connect to SQLite database using sqlite3
    with sqlite3.connect("my_lite_store.db") as conn:
        df.to_sql("cal_uni", conn, if_exists="replace", index=False)

# %%
def main_task():
    data = extract()
    df = transform(data)
    load(df)

# %%
main_task()

# %%
