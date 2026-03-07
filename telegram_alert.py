import requests
import sqlite3
import pandas as pd
import agro_core 

# ==========================================
# 📱 CONFIGURATION
# ==========================================
TELEGRAM_TOKEN = "8157128146:AAEWYg-Wou-339pLtP7UM9sQ_Orv-JxPItA"
COSMOFEED_LINK = "https://cosmofeed.com/vp/yourlink" # Your Paywall

CHANNELS = {
    "free": "-1003734990215",
    "vip_central": "-1003875234642", # MP & Chhattisgarh
    "vip_north": "-1003749980171"    # Haryana, Rajasthan, Punjab
}

VIP_REGIONS = {
    "vip_central": ["Madhya Pradesh", "Chattisgarh"],
    "vip_north": ["Haryana", "Rajasthan", "Punjab"]
}

def get_latest_date():
    conn = sqlite3.connect(agro_core.DB_NAME)
    dates_df = pd.read_sql_query("SELECT DISTINCT arrival_date FROM mandi_prices", conn)
    conn.close()
    if dates_df.empty: return None
    return pd.to_datetime(dates_df['arrival_date'], format='%d/%m/%Y').max().strftime('%d/%m/%Y')

def send_telegram(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})

def scan_for_deals(target_states=None, min_profit=1000):
    """
    If target_states is None, it scans ALL of India (for Free Channel).
    If target_states is a list, it scans only those origins (for VIP).
    """
    all_deals = []
    latest_date = get_latest_date()
    if not latest_date: return []

    conn = sqlite3.connect(agro_core.DB_NAME)
    crops = ['Tomato', 'Soybean', 'Paddy', 'Wheat', 'Mustard', 'Onion', 'Potato', 'Maize']
    
    for crop in crops:
        query = "SELECT state, market, modal_price FROM mandi_prices WHERE commodity LIKE ? AND arrival_date = ?"
        df = pd.read_sql_query(query, conn, params=[f'%{crop}%', latest_date]).drop_duplicates(subset=['market'])
        markets = df.to_dict('records')
        
        # Filter origins if we are doing a regional VIP scan
        origins = [m for m in markets if m['state'] in target_states] if target_states else markets
        
        for origin in origins:
            base_coords = agro_core.get_coordinates(origin['market'])
            if not base_coords: continue
            
            for target in markets:
                if origin['market'] == target['market']: continue
                target_coords = agro_core.get_coordinates(target['market'])
                if not target_coords: continue
                
                dist = agro_core.get_driving_distance(base_coords, target_coords, origin['market'], target['market'])
                if dist and dist <= 450: # Standard trucking range
                    fin = agro_core.calculate_real_profit(crop, dist, origin['modal_price'], target['modal_price'])
                    if fin["net_profit"] >= min_profit:
                        all_deals.append({
                            "crop": crop, "from": origin['market'], "to": target['market'],
                            "profit": fin["net_profit"], "dist": dist, "details": fin
                        })
    conn.close()
    return sorted(all_deals, key=lambda x: x['profit'], reverse=True)

def broadcast_free_top_3():
    print("📢 Calculating National Top 3...")
    deals = scan_for_deals(min_profit=2500)[:3] # High bar for free channel
    if not deals: return

    msg = "🏆 **TOP 3 NATIONAL ARBITRAGE OPPORTUNITIES** 🏆\n\n"
    for i, d in enumerate(deals, 1):
        msg += f"{i}. **{d['crop']}**: {d['from']} ➡️ {d['to']}\n   🔥 Net Profit: *₹{d['profit']:,.0f}/Qtl*\n\n"
    
    msg += f"📈 *Get full cost breakdowns and local routes for your state in VIP:*\n{COSMOFEED_LINK}"
    send_telegram(CHANNELS["free"], msg)

def broadcast_vip_reports():
    for channel_key, states in VIP_REGIONS.items():
        print(f"🔍 Scanning VIP Region: {channel_key}...")
        deals = scan_for_deals(target_states=states, min_profit=1000)
        
        if not deals:
            continue

        msg = f"🚜 **{channel_key.replace('_', ' ').upper()} - DAILY TRADE REPORT**\n"
        msg += f"Found {len(deals)} profitable routes today.\n\n"
        
        for d in deals[:8]: # Send up to 8 best routes for the state
            f = d['details']
            msg += (
                f"📍 **{d['crop']}**: {d['from']} ➡️ {d['to']}\n"
                f"💰 Profit: *₹{d['profit']:,.0f}* | Dist: {d['dist']:.0f}km\n"
                f"   (Freight: -₹{f['freight']:.0f} | Fees: -₹{f['fees_and_labor']:.0f})\n\n"
            )
        send_telegram(CHANNELS[channel_key], msg)

if __name__ == "__main__":
    broadcast_free_top_3()
    broadcast_vip_reports()
