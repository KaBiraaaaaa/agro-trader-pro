import sqlite3
import pandas as pd
import requests
from geopy.geocoders import Nominatim

# ==========================================
# ⚙️ CENTRAL CONFIGURATION
# ==========================================
DB_NAME = "agro_data.db"
TRUSTED_STATES = ["Haryana", "Rajasthan", "Andhra Pradesh", "Telangana", "Madhya Pradesh", "Chhattisgarh"]

CROP_PROFILES = {
    "tomato": {"wastage": 0.08, "labor": 20},
    "onion": {"wastage": 0.05, "labor": 18},
    "paddy": {"wastage": 0.01, "labor": 12}, 
    "wheat": {"wastage": 0.01, "labor": 12},
    "soybean": {"wastage": 0.01, "labor": 15}, 
    "mustard": {"wastage": 0.01, "labor": 15},
    "maize": {"wastage": 0.02, "labor": 12},
    "cotton": {"wastage": 0.00, "labor": 25}, 
    "default": {"wastage": 0.03, "labor": 15}
}

geolocator = Nominatim(user_agent="agro_pro_v3")

# --- THE NEW CACHING SYSTEM ---
def _setup_cache_tables():
    """Silently creates the memory tables inside your database if they don't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS location_cache (city_name TEXT PRIMARY KEY, lat REAL, lon REAL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS route_cache (origin TEXT, destination TEXT, distance_km REAL, UNIQUE(origin, destination))''')
    conn.commit()
    conn.close()

# Run setup immediately when this file is loaded
_setup_cache_tables()

def get_coordinates(city_name):
    """Checks the local database first. If missing, fetches from internet and saves it."""
    clean_name = city_name.split('(')[0].replace('APMC', '').replace('Veg', '').strip()
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Check local cache (Instant)
    cursor.execute("SELECT lat, lon FROM location_cache WHERE city_name = ?", (clean_name,))
    cached = cursor.fetchone()
    if cached:
        conn.close()
        return cached 
        
    # 2. Ask internet (Slow)
    try:
        location = geolocator.geocode(f"{clean_name}, India")
        if location:
            # 3. Save to local DB for next time
            cursor.execute("INSERT OR REPLACE INTO location_cache (city_name, lat, lon) VALUES (?, ?, ?)", 
                           (clean_name, location.latitude, location.longitude))
            conn.commit()
            conn.close()
            return (location.latitude, location.longitude)
    except Exception as e:
        pass
    
    conn.close()
    return None

def get_driving_distance(c1, c2, city1_name, city2_name):
    """Checks local database for the route. If missing, calculates via OSRM and saves it."""
    clean_city1 = city1_name.split('(')[0].replace('APMC', '').replace('Veg', '').strip()
    clean_city2 = city2_name.split('(')[0].replace('APMC', '').replace('Veg', '').strip()
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Check local cache (Instant)
    cursor.execute("SELECT distance_km FROM route_cache WHERE origin = ? AND destination = ?", (clean_city1, clean_city2))
    cached = cursor.fetchone()
    if cached:
        conn.close()
        return cached[0]
        
    # 2. Ask internet (Slow)
    try:
        url = f"http://router.project-osrm.org/route/v1/driving/{c1[1]},{c1[0]};{c2[1]},{c2[0]}?overview=false"
        res = requests.get(url).json()
        if res.get('code') == 'Ok':
            dist = res['routes'][0]['distance'] / 1000.0
            
            # 3. Save BOTH directions to DB to make future queries 2x faster
            cursor.execute("INSERT OR IGNORE INTO route_cache (origin, destination, distance_km) VALUES (?, ?, ?)", (clean_city1, clean_city2, dist))
            cursor.execute("INSERT OR IGNORE INTO route_cache (origin, destination, distance_km) VALUES (?, ?, ?)", (clean_city2, clean_city1, dist))
            conn.commit()
            conn.close()
            return dist
    except Exception as e:
        pass
        
    conn.close()
    return None

def fetch_trusted_data(commodity_query):
    try:
        conn = sqlite3.connect(DB_NAME)
        placeholders = ', '.join(['?'] * len(TRUSTED_STATES))
        query = f"SELECT market, modal_price FROM mandi_prices WHERE commodity LIKE ? AND state IN ({placeholders}) ORDER BY arrival_date DESC"
        params = [f'%{commodity_query}%'] + TRUSTED_STATES
        df = pd.read_sql_query(query, conn, params=params).drop_duplicates(subset=['market'])
        conn.close()
        return df.to_dict('records')
    except Exception as e:
        return []

def calculate_real_profit(commodity, distance_km, buy_price_qtl, sell_price_qtl, custom_freight=None, custom_tax=None, custom_labor=None):
    profile = CROP_PROFILES.get(commodity.lower(), CROP_PROFILES["default"])
    freight_rate = custom_freight if custom_freight is not None else 35
    tax_rate = custom_tax if custom_tax is not None else 0.03
    labor_rate = custom_labor if custom_labor is not None else profile["labor"]
    truck_capacity_qtl = 100  
    sellable_qty = truck_capacity_qtl * (1 - profile["wastage"]) 
    total_buy_cost = buy_price_qtl * truck_capacity_qtl
    total_sell_revenue = sell_price_qtl * sellable_qty
    freight_cost = distance_km * freight_rate
    total_labor = labor_rate * truck_capacity_qtl
    mandi_fees = (total_buy_cost + total_sell_revenue) * tax_rate
    net_profit = total_sell_revenue - total_buy_cost - freight_cost - total_labor - mandi_fees
    
    return {
        "net_profit": net_profit, "gross_profit": (sell_price_qtl - buy_price_qtl) * truck_capacity_qtl - freight_cost,
        "wastage_loss": (truck_capacity_qtl * profile["wastage"]) * sell_price_qtl,
        "fees_and_labor": total_labor + mandi_fees, "freight": freight_cost
    }
def analyze_state_volatility():
    """Finds the state and commodity with the most extreme price gap for Macro Alerts."""
    try:
        conn = sqlite3.connect(DB_NAME)
        # 1. Find the latest date to ensure we only broadcast fresh data
        latest_date_str = pd.read_sql_query("SELECT MAX(arrival_date) FROM mandi_prices", conn).iloc[0, 0]
        if not latest_date_str: return None

        # 2. Calculate the price gaps across all states
        query = f"""
        SELECT state, commodity, 
               MIN(modal_price) as min_price, 
               MAX(modal_price) as max_price,
               (MAX(modal_price) - MIN(modal_price)) as price_gap
        FROM mandi_prices
        WHERE arrival_date = '{latest_date_str}'
        GROUP BY state, commodity
        HAVING price_gap > 500  -- Only flag gaps larger than ₹500/Qtl
        ORDER BY price_gap DESC
        LIMIT 1
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            return df.iloc[0].to_dict()
    except Exception as e:
        print(f"Volatility Error: {e}")
    return None
