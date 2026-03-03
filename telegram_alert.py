import requests
import sqlite3
import pandas as pd
import agro_core 

# ==========================================
# 📱 TELEGRAM & FUNNEL CONFIGURATION
# ==========================================
TELEGRAM_TOKEN = "8157128146:AAEWYg-Wou-339pLtP7UM9sQ_Orv-JxPItA" 
COSMOFEED_LINK = "https://cosmofeed.com/your-payment-link" 

CHANNELS = {
    "free_marketing": "-1003734990215", 
    "vip_north": "-1003749980171",      
    "vip_central": "-1003875234642"     
}

# The Bot's Daily VIP Job List (Now State-Wide!)
VIP_TASKS = {
    "vip_central": {
        "chat_id": CHANNELS["vip_central"],
        "states": ["Madhya Pradesh", "Chhattisgarh"], 
        "crops": ["Tomato", "Soybean", "Paddy", "Onion"]
    },
    "vip_north": {
        "chat_id": CHANNELS["vip_north"],
        "states": ["Haryana", "Rajasthan", "Punjab"],
        "crops": ["Wheat", "Mustard", "Cotton", "Maize"]
    }
}

def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    res = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    if res.status_code != 200:
        print(f"❌ Failed to send to {chat_id}: {res.text}")

def broadcast_macro_alert():
    print("📢 Analyzing market volatility for Free Channel...")
    chaos = agro_core.analyze_state_volatility()
    
    if chaos:
        message = (
            f"🚨 **MASSIVE MARKET SHIFT DETECTED** 🚨\n\n"
            f"Extreme volatility spotted in the **{chaos['state']}** market for **{chaos['commodity']}** today.\n\n"
            f"📉 Lowest Mandi: ₹{chaos['min_price']}/Qtl\n"
            f"📈 Highest Mandi: ₹{chaos['max_price']}/Qtl\n"
            f"🔥 **PRICE GAP: ₹{chaos['price_gap']}/Qtl**\n\n"
            f"Our software has mapped the exact trucking routes to exploit this gap. "
            f"Log into the VIP channels to get the point-to-point execution plans and hidden cost breakdowns.\n\n"
            f"👇 **Unlock VIP Access Here:**\n{COSMOFEED_LINK}"
        )
        send_message(CHANNELS["free_marketing"], message)
        print("✅ Free Macro Alert broadcasted!")
    else:
        print("📉 Markets are stable today. No macro alert sent.")

def find_best_statewide_route(states, crops):
    """Scans ENTIRE states for arbitrage instead of just hardcoded cities."""
    best_deal = None
    highest_profit = 3000 

    conn = sqlite3.connect(agro_core.DB_NAME)
    
    for crop in crops:
        query = "SELECT state, market, modal_price FROM mandi_prices WHERE commodity LIKE ?"
        df = pd.read_sql_query(query, conn, params=[f'%{crop}%']).drop_duplicates(subset=['market'])
        markets = df.to_dict('records')
        
        if not markets: continue
        
        # Isolate the origins to ONLY markets inside our target VIP states
        origin_markets = [m for m in markets if m['state'] in states]
        
        for origin in origin_markets:
            base_coords = agro_core.get_coordinates(origin['market'])
            if not base_coords: continue
            
            for target in markets:
                if origin['market'] == target['market']: continue
                
                target_coords = agro_core.get_coordinates(target['market'])
                if not target_coords: continue
                
                distance = agro_core.get_driving_distance(base_coords, target_coords, origin['market'], target['market'])
                if distance and distance <= 400:
                    financials = agro_core.calculate_real_profit(crop, distance, origin['modal_price'], target['modal_price'])
                    
                    if financials["net_profit"] > highest_profit:
                        highest_profit = financials["net_profit"]
                        best_deal = {
                            "base_city": origin['market'], "target_market": target['market'], "crop": crop,
                            "buy": origin['modal_price'], "sell": target['modal_price'], 
                            "distance": distance, "financials": financials
                        }
    conn.close()
    return best_deal

def broadcast_vip_alerts():
    for region_name, task in VIP_TASKS.items():
        print(f"🔍 Scanning entire states {task['states']} for {region_name}...")
        deal = find_best_statewide_route(task["states"], task["crops"])
        
        if deal:
            f = deal["financials"]
            message = (
                f"🚜 *VIP ROUTE EXECUTION*\n\n"
                f"📍 *Route:* {deal['base_city']} ➡️ {deal['target_market']}\n"
                f"🌾 *Crop:* {deal['crop']} ({deal['distance']:.1f} km)\n\n"
                f"💰 *Buy Local:* ₹{deal['buy']}/Qtl | *Sell There:* ₹{deal['sell']}/Qtl\n\n"
                f"⚠️ *Hidden Cost Breakdown:*\n"
                f"🚛 Freight & Tolls: -₹{f['freight']:,.0f}\n"
                f"🥀 Est. Spoilage Loss: -₹{f['wastage_loss']:,.0f}\n"
                f"⚖️ Mandi Fees & Labor: -₹{f['fees_and_labor']:,.0f}\n\n"
                f"🔥 *TRUE NET PROFIT:* ₹{f['net_profit']:,.0f}"
            )
            send_message(task["chat_id"], message)
            print(f"✅ VIP Alert sent to {region_name}!")
        else:
            print(f"📉 No high-profit routes found for {region_name} right now.")

if __name__ == "__main__":
    print("🚀 Starting Daily Agro Broadcasting Sequence...")
    broadcast_macro_alert()
    broadcast_vip_alerts()
    print("🏁 Sequence Complete.")
