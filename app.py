import streamlit as st
import pandas as pd
import agro_core  

# --- STREAMLIT UI SETUP ---
st.set_page_config(page_title="Agro Trader Pro", page_icon="🌾", layout="wide")

st.title("🌾 Agro Trader Pro (Enterprise Edition)")
st.markdown("Find the most profitable agricultural arbitrage routes in real-time, with full hidden-cost breakdowns.")

# --- SIDEBAR: SEARCH PARAMETERS ---
st.sidebar.header("Search Parameters")
my_location = st.sidebar.text_input("Base City (e.g., Raigarh, Raipur)", value="Raigarh").strip()
commodity = st.sidebar.text_input("Commodity (e.g., Tomato, Paddy)", value="Tomato").strip()
min_profit = st.sidebar.slider("Minimum True Net Profit (₹)", 1000, 50000, 5000, step=1000)

# --- SIDEBAR: ADVANCED LOGISTICS (OVERRIDES) ---
st.sidebar.markdown("---")
with st.sidebar.expander("⚙️ Advanced Logistics (Overrides)", expanded=False):
    st.caption("Leave as 0.0 to use system defaults based on crop type.")
    custom_freight = st.number_input("Custom Truck Rate (₹/km)", min_value=0.0, value=0.0, step=1.0)
    custom_tax = st.number_input("Custom Mandi Tax (%)", min_value=0.0, value=0.0, step=0.5)
    custom_labor = st.number_input("Custom Labor Rate (₹/Qtl)", min_value=0.0, value=0.0, step=1.0)

freight_val = custom_freight if custom_freight > 0 else None
tax_val = (custom_tax / 100) if custom_tax > 0 else None
labor_val = custom_labor if custom_labor > 0 else None

# --- MAIN EXECUTION ---
if st.sidebar.button("Analyze Routes 🚀"):
    with st.spinner("Querying database and calculating enterprise logistics..."):
        
        markets = agro_core.fetch_trusted_data(commodity)
        
        if not markets:
            st.error(f"No reliable data found for '{commodity}'. Check your database or spelling.")
            st.stop()
            
        base_coords = agro_core.get_coordinates(my_location)
        if not base_coords:
            st.error(f"Could not map '{my_location}'. Try a nearby larger city.")
            st.stop()

        local_market = next((m for m in markets if my_location.lower() in m['market'].lower()), None)
        if not local_market:
            st.warning(f"No local prices for {my_location} in the database today. Showing all regional data instead.")
            st.dataframe(pd.DataFrame(markets))
            st.stop()

        local_price = local_market['modal_price']
        st.success(f"**Local Buy Price in {local_market['market']}:** ₹{local_price}/Qtl")
        
        opportunities = []
        progress_bar = st.progress(0)
        
        for i, m in enumerate(markets):
            progress_bar.progress((i + 1) / len(markets))
            
            if m['market'] == local_market['market']: 
                continue
            
            target_coords = agro_core.get_coordinates(m['market'])
            if not target_coords: 
                continue
            
            # Utilizing the new caching distance function
            distance = agro_core.get_driving_distance(base_coords, target_coords, my_location, m['market'])
            if not distance or distance > 400: 
                continue
            
            financials = agro_core.calculate_real_profit(
                commodity=commodity, 
                distance_km=distance, 
                buy_price_qtl=local_price, 
                sell_price_qtl=m['modal_price'],
                custom_freight=freight_val,
                custom_tax=tax_val,
                custom_labor=labor_val
            )
            
            if financials['net_profit'] >= min_profit:
                opportunities.append({
                    "Action": "SELL TO", 
                    "Market": m['market'], 
                    "Distance (km)": round(distance, 1),
                    "Buy Price": local_price, 
                    "Sell Price": m['modal_price'], 
                    "Gross Margin": financials['gross_profit'],
                    "Freight": financials['freight'],
                    "Spoilage Loss": financials['wastage_loss'],
                    "Fees & Labor": financials['fees_and_labor'],
                    "True Net Profit (₹)": financials['net_profit']
                })
                
        if opportunities:
            df_results = pd.DataFrame(opportunities).sort_values(by="True Net Profit (₹)", ascending=False)
            currency_cols = ["Buy Price", "Sell Price", "Gross Margin", "Freight", "Spoilage Loss", "Fees & Labor", "True Net Profit (₹)"]
            for col in currency_cols:
                df_results[col] = df_results[col].apply(lambda x: f"₹{x:,.0f}")
            
            st.dataframe(df_results, use_container_width=True)
            st.balloons()
        else:
            st.info("📉 No profitable routes found matching your criteria after deducting all taxes, fees, and spoilage.")
