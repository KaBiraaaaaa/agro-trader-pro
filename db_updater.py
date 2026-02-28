import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta

# ==========================================
# 🔄 DATABASE UPDATER CONFIGURATION
# ==========================================
DB_NAME = "agro_data.db"
# You will get this free key from data.gov.in
API_KEY = "579b464db66ec23bdd0000017c689950564f49c55cd9635e63c4ae8e" 
# The official endpoint for real-time Mandi prices
API_URL = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={API_KEY}&format=json&limit=2000"

def fetch_fresh_mandi_data():
    """Pulls the latest agricultural prices directly from the Government of India API."""
    print("📡 Contacting Govt. of India API for fresh Mandi prices...")
    
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            records = data.get('records', [])
            
            if not records:
                print("⚠️ API returned empty records today.")
                return pd.DataFrame()
                
            # Convert the raw JSON into a clean Pandas DataFrame
            df = pd.DataFrame(records)
            
            # Rename government columns to match our database structure
            df.rename(columns={
                'state': 'state',
                'market': 'market',
                'commodity': 'commodity',
                'modal_price': 'modal_price',
                'arrival_date': 'arrival_date'
            }, inplace=True)
            
            # Ensure price is a number
            df['modal_price'] = pd.to_numeric(df['modal_price'], errors='coerce')
            # Drop any rows where price data is missing
            df.dropna(subset=['modal_price'], inplace=True)
            
            print(f"✅ Successfully downloaded {len(df)} fresh records.")
            return df
        else:
            print(f"❌ API Connection Failed: Status Code {response.status_code}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Network Error: {e}")
        return pd.DataFrame()

def update_database(fresh_df):
    """Saves the new prices and deletes old data to keep the database fast."""
    if fresh_df.empty:
        print("⏭️ No new data to update.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Ensure our table exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mandi_prices (
            state TEXT, market TEXT, commodity TEXT, 
            modal_price REAL, arrival_date TEXT
        )
    ''')
    
   # 2. Add the fresh data safely (Update if exists, Insert if new)
    data_to_insert = fresh_df[['state', 'market', 'commodity', 'modal_price', 'arrival_date']].values.tolist()

    cursor.executemany('''
        INSERT OR REPLACE INTO mandi_prices (state, market, commodity, modal_price, arrival_date)
        VALUES (?, ?, ?, ?, ?)
    ''', data_to_insert)
    
    # 3. Clean up the database (Delete data older than 7 days)
    # This is crucial so your GitHub repository doesn't run out of storage space
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%d/%m/%Y')
    cursor.execute("DELETE FROM mandi_prices WHERE arrival_date < ?", (seven_days_ago,))
    
    # 4. Remove duplicate entries (same market, same crop, same day)
    cursor.execute("""
        DELETE FROM mandi_prices 
        WHERE rowid NOT IN (
            SELECT MAX(rowid) 
            FROM mandi_prices 
            GROUP BY state, market, commodity, arrival_date
        )
    """)
    
    conn.commit()
    conn.close()
    print("💾 Database updated and optimized successfully!")

if __name__ == "__main__":
    print("🚀 Starting Daily Database Update Sequence...")
    fresh_data = fetch_fresh_mandi_data()
    update_database(fresh_data)
    print("🏁 Sequence Complete.")
