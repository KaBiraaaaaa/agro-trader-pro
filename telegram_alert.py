import requests
import sqlite3
import pandas as pd
import agro_core 

# ==========================================
# 📱 CONFIGURATION
# ==========================================
TELEGRAM_TOKEN = "8157128146:AAEWYg-Wou-339pLtP7UM9sQ_Orv-JxPItA"
COSMOFEED_LINK = "https://cosmofeed.com/vp/yourlink"

CHANNELS = {
    "free": "-1003734990215",
    "vip_central": "-1003875234642", 
    "vip_north": "-1003749980171"    
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

def scan_for_deals(target_states, min_profit=1000):
    """Scans for deals ONLY within the specified regional states to prevent infinite loops."""
    all_deals = []
    latest_date = get_latest_date()
    if not latest_date: return []

    conn = sqlite3.connect(agro_core.DB_NAME)
    crops = ['Tomato', 'Soybean', 'Paddy', 'Wheat', 'Mustard', 'Onion', 'Potato', 'Maize']
    
    for crop in crops:
        query = "SELECT state, market, modal_price FROM mandi_prices WHERE commodity LIKE ? AND arrival_date = ?"
        df = pd.read_sql_query(query, conn, params=[f'%{crop}%', latest_date]).drop_duplicates(subset=['market'])
        markets = df.to_dict('records')
        
        # Limit both origin and destination to the region to cut math calculations by 90%
        regional_markets = [m for m in markets if m['state'] in target_states]
        
        for origin in regional_markets:
            base_coords = agro_core.get_coordinates(origin['market'])
            if not base_coords: continue
            
            for target in regional_markets:
                if origin['market'] == target['market']: continue
                target_coords = agro_core.get_coordinates(target['market'])
                if not target_coords: continue
                
                dist = agro_core.get_driving_distance(base_coords, target_coords, origin['market'], target['market'])
                if dist and dist <= 450: 
                    fin = agro_core.calculate_real_profit(crop, dist, origin['modal_price'], target['modal_price'])
                    if fin["net_profit"] >= min_profit:
                        all_deals.append({
                            "crop": crop, "from": origin['market'], "to": target['market'],
                            "profit": fin["net_profit"], "dist": dist, "details": fin
                        })
    conn.close()
    return sorted(all_deals, key=lambda x: x['profit'], reverse=True)

def run_daily_broadcast():
    all_national_deals = []

    # 1. Process VIP Channels First
    for channel_key, states in VIP_REGIONS.items():
        print(f"🔍 Scanning VIP Region: {channel_key}...")
        deals = scan_for_deals(target_states=states, min_profit=1000)
        
        if not deals:
            continue

        # Save these deals to a master list so we can recycle them for the Free channel
        all_national_deals.extend(deals) 

        msg = f"🚜 **{channel_key.replace('_', ' ').upper()} - DAILY TRADE REPORT**\n"
        msg += f"Found {len(deals)} profitable routes today.\n\n"
        
        for d in deals[:8]: 
            f = d['details']
            msg += (
                f"📍 **{d['crop']}**: {d['from']} ➡️ {d['to']}\n"
                f"💰 Profit: *₹{d['profit']:,.0f}* | Dist: {d['dist']:.0f}km\n"
                f"   (Freight: -₹{f['freight']:.0f} | Fees: -₹{f['fees_and_labor']:.0f})\n\n"
            )
        send_telegram(CHANNELS[channel_key], msg)
        print(f"✅ VIP Alert sent to {channel_key}!")

    # 2. Recycle the best data for the Free Channel
    if all_national_deals:
        print("📢 Broadcasting Free Top 3...")
        # Sort all the collected regional deals by highest profit
        top_3 = sorted(all_national_deals, key=lambda x: x['profit'], reverse=True)[:3]
        
        msg = "🏆 **TOP 3 REGIONAL ARBITRAGE OPPORTUNITIES** 🏆\n\n"
        for i, d in enumerate(top_3, 1):
            msg += f"{i}. **{d['crop']}**: {d['from']} ➡️ {d['to']}\n   🔥 Net Profit: *₹{d['profit']:,.0f}*\n\n"
        
        msg += f"📈 *Get full cost breakdowns and local routes for your state in VIP:*\n{COSMOFEED_LINK}"
        send_telegram(CHANNELS["free"], msg)
        print("✅ Free Top 3 Alert sent!")

if __name__ == "__main__":
    run_daily_broadcast()
