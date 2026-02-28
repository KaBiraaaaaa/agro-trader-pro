import requests
import agro_core 

# ==========================================
# 📱 TELEGRAM & FUNNEL CONFIGURATION
# ==========================================
TELEGRAM_TOKEN = "8157128146:AAEWYg-Wou-339pLtP7UM9sQ_Orv-JxPItA" 
COSMOFEED_LINK = "https://cosmofeed.com/your-payment-link" # Your future paywall link

CHANNELS = {
    "free_marketing": "-1003734990215", # Agro Trader Pro
    "vip_north": "-1003749980171",      # Agro Trader VIP: Haryana & Rajasthan
    "vip_central": "-1003875234642"     # Agro Trader VIP: MP & CG
}

# The Bot's Daily VIP Job List
VIP_TASKS = {
    "vip_central": {
        "chat_id": CHANNELS["vip_central"],
        "hubs": ["Raipur", "Indore", "Raigarh"], 
        "crops": ["Tomato", "Soybean", "Paddy"]
    },
    "vip_north": {
        "chat_id": CHANNELS["vip_north"],
        "hubs": ["Karnal", "Jaipur", "Rohtak"],
        "crops": ["Wheat", "Mustard", "Cotton"]
    }
}

def send_message(chat_id, text):
    """Helper function to shoot messages to Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    res = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    if res.status_code != 200:
        print(f"❌ Failed to send to {chat_id}: {res.text}")

def broadcast_macro_alert():
    """Generates the Free Channel marketing hook based on market volatility."""
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

def find_best_regional_route(hubs, crops):
    """Scans multiple cities and crops to find the single best route for a VIP region."""
    best_deal = None
    highest_profit = 3000 # Minimum threshold for a VIP alert

    for crop in crops:
        markets = agro_core.fetch_trusted_data(crop)
        if not markets: continue
        
        for base_city in hubs:
            base_coords = agro_core.get_coordinates(base_city)
            if not base_coords: continue
            
            local_market = next((m for m in markets if base_city.lower() in m['market'].lower()), None)
            if not local_market: continue
            
            local_price = local_market['modal_price']
            
            for m in markets:
                if m['market'] == local_market['market']: continue
                target_coords = agro_core.get_coordinates(m['market'])
                if not target_coords: continue
                
                distance = agro_core.get_driving_distance(base_coords, target_coords, base_city, m['market'])
                if distance and distance <= 400:
                    financials = agro_core.calculate_real_profit(crop, distance, local_price, m['modal_price'])
                    if financials["net_profit"] > highest_profit:
                        highest_profit = financials["net_profit"]
                        best_deal = {
                            "base_city": base_city, "target_market": m['market'], "crop": crop,
                            "buy": local_price, "sell": m['modal_price'], 
                            "distance": distance, "financials": financials
                        }
    return best_deal

def broadcast_vip_alerts():
    """Loops through VIP regions and sends hyper-specific routes."""
    for region_name, task in VIP_TASKS.items():
        print(f"🔍 Calculating optimal routes for {region_name}...")
        deal = find_best_regional_route(task["hubs"], task["crops"])
        
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
