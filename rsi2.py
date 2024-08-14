import pandas as pd
import numpy as np
from tradingview_ta import TA_Handler, Interval
from datetime import datetime, timezone
import time
import logging
from cachetools import TTLCache
import os
import concurrent.futures
import streamlit as st
import plotly.express as px
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up caching
cache = TTLCache(maxsize=1000, ttl=300)  # Cache with 5-minute TTL

# CSV file path
CSV_FILE_PATH = "15m_rsi.csv"

# List of symbols to analyze
symbols = [
    "10000LADYSUSDT.P", "10000NFTUSDT.P", "1000BONKUSDT.P", "1000BTTUSDT.P", 
    "1000FLOKIUSDT.P", "1000LUNCUSDT.P", "1000PEPEUSDT.P", "1000XECUSDT.P", 
    "1INCHUSDT.P", "AAVEUSDT.P", "ACHUSDT.P", "ADAUSDT.P", "AGLDUSDT.P", 
    "AKROUSDT.P", "ALGOUSDT.P", "ALICEUSDT.P", "ALPACAUSDT.P", 
    "ALPHAUSDT.P", "AMBUSDT.P", "ANKRUSDT.P", "APEUSDT.P", "API3USDT.P", 
    "APTUSDT.P", "ARUSDT.P", "ARBUSDT.P", "ARKUSDT.P", 
    "ARKMUSDT.P", "ARPAUSDT.P", "ASTRUSDT.P", "ATAUSDT.P", "ATOMUSDT.P", 
    "AUCTIONUSDT.P", "AUDIOUSDT.P", "AVAXUSDT.P", "AXSUSDT.P", "BADGERUSDT.P", 
    "BAKEUSDT.P", "BALUSDT.P", "BANDUSDT.P", "BATUSDT.P", "BCHUSDT.P", 
    "BELUSDT.P", "BICOUSDT.P", "BIGTIMEUSDT.P", "BLURUSDT.P", "BLZUSDT.P",
    "BTCUSDT.P", "C98USDT.P", "CEEKUSDT.P", "CELOUSDT.P", "CELRUSDT.P", "CFXUSDT.P",
    "CHRUSDT.P", "CHZUSDT.P", "CKBUSDT.P", "COMBOUSDT.P", "COMPUSDT.P",
    "COREUSDT.P", "COTIUSDT.P", "CROUSDT.P", "CRVUSDT.P", "CTCUSDT.P",
    "CTKUSDT.P", "CTSIUSDT.P", "CVCUSDT.P", "CVXUSDT.P", "CYBERUSDT.P", "DARUSDT.P",
    "DASHUSDT.P", "DENTUSDT.P", "DGBUSDT.P", "DODOUSDT.P", "DOGEUSDT.P", "DOTUSDT.P",
    "DUSKUSDT.P", "DYDXUSDT.P", "EDUUSDT.P", "EGLDUSDT.P", "ENJUSDT.P", "ENSUSDT.P",
    "EOSUSDT.P", "ETCUSDT.P", "ETHUSDT.P", "ETHWUSDT.P", "FILUSDT.P",
    "FITFIUSDT.P", "FLOWUSDT.P", "FLRUSDT.P", "FORTHUSDT.P", "FRONTUSDT.P", "FTMUSDT.P",
    "FXSUSDT.P", "GALAUSDT.P", "GFTUSDT.P", "GLMUSDT.P",
    "GLMRUSDT.P", "GMTUSDT.P", "GMXUSDT.P", "GRTUSDT.P", "GTCUSDT.P", "HBARUSDT.P", 
    "HFTUSDT.P", "HIFIUSDT.P", "HIGHUSDT.P", "HNTUSDT.P",
    "HOOKUSDT.P", "HOTUSDT.P", "ICPUSDT.P", "ICXUSDT.P", "IDUSDT.P", "IDEXUSDT.P",
    "ILVUSDT.P", "IMXUSDT.P", "INJUSDT.P", "IOSTUSDT.P", "IOTAUSDT.P", "IOTXUSDT.P",
    "JASMYUSDT.P", "JOEUSDT.P", "JSTUSDT.P", "KASUSDT.P", "KAVAUSDT.P", "KDAUSDT.P",
    "KEYUSDT.P", "KLAYUSDT.P", "KNCUSDT.P", "KSMUSDT.P", "LDOUSDT.P", "LEVERUSDT.P",
    "LINAUSDT.P", "LINKUSDT.P", "LITUSDT.P", "LOOKSUSDT.P", "LOOMUSDT.P", "LPTUSDT.P",
    "LQTYUSDT.P", "LRCUSDT.P", "LTCUSDT.P", "LUNA2USDT.P", "MAGICUSDT.P",
    "MANAUSDT.P", "MASKUSDT.P", "MATICUSDT.P", "MAVUSDT.P", "MDTUSDT.P",
    "MINAUSDT.P", "MKRUSDT.P", "MNTUSDT.P", "MTLUSDT.P", "NEARUSDT.P",
    "NEOUSDT.P", "NKNUSDT.P", "NMRUSDT.P", "NTRNUSDT.P", "OGUSDT.P",
    "OGNUSDT.P", "OMGUSDT.P", "ONEUSDT.P", "ONTUSDT.P", "OPUSDT.P", "ORBSUSDT.P",
    "ORDIUSDT.P", "OXTUSDT.P", "PAXGUSDT.P", "PENDLEUSDT.P", "PEOPLEUSDT.P", "PERPUSDT.P",
    "PHBUSDT.P", "PROMUSDT.P", "PONKEUSDT.P", "QNTUSDT.P", "QTUMUSDT.P", "RADUSDT.P", "RDNTUSDT.P", 
    "REEFUSDT.P", "RENUSDT.P", "REQUSDT.P", "RLCUSDT.P", "ROSEUSDT.P", 
    "RPLUSDT.P", "RSRUSDT.P", "RSS3USDT.P", "RUNEUSDT.P", "RVNUSDT.P",
    "SANDUSDT.P", "SCUSDT.P", "SCRTUSDT.P", "SEIUSDT.P", "SFPUSDT.P", "SHIB1000USDT.P",
    "SKLUSDT.P", "SLPUSDT.P", "SNXUSDT.P", "SOLUSDT.P", "SPELLUSDT.P", "SSVUSDT.P", 
    "STGUSDT.P", "STMXUSDT.P", "STORJUSDT.P", "STPTUSDT.P", "STXUSDT.P", "SUIUSDT.P", 
    "SUNUSDT.P", "SUSHIUSDT.P", "SWEATUSDT.P", "SXPUSDT.P",
    "TUSDT.P", "THETAUSDT.P", "TLMUSDT.P", "TOMIUSDT.P", "TONUSDT.P",
    "TRBUSDT.P", "TRUUSDT.P", "TRXUSDT.P", "TWTUSDT.P", "UMAUSDT.P", "UNFIUSDT.P",
    "UNIUSDT.P", "USDCUSDT.P", "VETUSDT.P", "VGXUSDT.P", "VRAUSDT.P",
    "WAVESUSDT.P", "WAXPUSDT.P", "WLDUSDT.P", "WOOUSDT.P", "XCNUSDT.P",
    "XEMUSDT.P", "XLMUSDT.P", "XMRUSDT.P", "XNOUSDT.P", "XRPUSDT.P", "XTZUSDT.P",
    "XVGUSDT.P", "XVSUSDT.P", "YFIUSDT.P", "YGGUSDT.P", "ZECUSDT.P", "ZENUSDT.P", "ZILUSDT.P", "ZRXUSDT.P"
]

# Configuration
exchange = "BYBIT"
screener = "crypto"
interval = Interval.INTERVAL_15_MINUTES

def fetch_all_data(symbol, exchange, screener, interval):
    cache_key = f"{symbol}_{exchange}_{screener}_{interval}"
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        handler = TA_Handler(
            symbol=symbol,
            exchange=exchange,
            screener=screener,
            interval=interval,
            timeout=None
        )
        analysis = handler.get_analysis()
        cache[cache_key] = analysis
        return analysis
    except Exception as e:
        logging.error(f"Error fetching data for {symbol} on {interval}: {str(e)}")
        return None

def process_symbol(symbol):
    analysis = fetch_all_data(symbol, exchange, screener, interval)
    
    if analysis:
        rsi_value = analysis.indicators.get('RSI', None)  # Fetch RSI value
        return {"Symbol": symbol, "15m RSI": rsi_value}
    return None

def update_csv():
    while True:
        try:
            current_datetime = datetime.now(timezone.utc)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_symbol = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
                results = []
                for future in concurrent.futures.as_completed(future_to_symbol):
                    result = future.result()
                    if result:
                        result["Timestamp"] = current_datetime
                        results.append(result)
            
            new_df = pd.DataFrame(results)
            
            # Reorder columns to ensure '15m RSI' is in the correct position
            columns = [col for col in new_df.columns if col != '15m RSI'] + ['15m RSI']
            new_df = new_df[columns]
            
            # Append the new data to the CSV file
            if os.path.exists(CSV_FILE_PATH):
                new_df.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)
            else:
                new_df.to_csv(CSV_FILE_PATH, index=False)
            
            logging.info(f"CSV updated at {current_datetime} with {len(results)} symbols.")
            
            # Sleep for 3 minutes before the next update
            time.sleep(180)
            
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            time.sleep(600)  # Wait 10 minutes before retrying

def load_and_process_data():
    df = pd.read_csv(CSV_FILE_PATH)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', utc=True)
    df = df.dropna(subset=['Timestamp'])
    df['15m RSI'] = pd.to_numeric(df['15m RSI'], errors='coerce')
    return df

def get_latest_data(df):
    latest_timestamp = df['Timestamp'].max()
    latest_df = df[df['Timestamp'] == latest_timestamp].copy()
    latest_df['RSI Change'] = latest_df.groupby('Symbol')['15m RSI'].transform(lambda x: x.pct_change() * 100)
    return latest_df, latest_timestamp

def display_streamlit_app():
    st.set_page_config(layout="wide")
    st.title('RSI 15-Minute Interval Dashboard')

    if os.path.exists(CSV_FILE_PATH):
        try:
            df = load_and_process_data()
            latest_df, latest_timestamp = get_latest_data(df)
            
            st.sidebar.header('Last Update')
            st.sidebar.write(f"Last update: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}")

            # Create different dataframes based on RSI values
            df_above_70 = latest_df[latest_df['15m RSI'] > 70].sort_values(by='15m RSI', ascending=False)
            df_below_30 = latest_df[latest_df['15m RSI'] < 30].sort_values(by='15m RSI', ascending=True)
            df_between_45_55 = latest_df[(latest_df['15m RSI'] >= 45) & (latest_df['15m RSI'] <= 55)]

            # Separate rising and decreasing RSI for 45-55 range
            df_rising = df_between_45_55[df_between_45_55['RSI Change'] > 0].sort_values(by='RSI Change', ascending=False)
            df_decreasing = df_between_45_55[df_between_45_55['RSI Change'] < 0].sort_values(by='RSI Change', ascending=True)

            # Create two columns for the layout
            col1, col2 = st.columns(2)

            with col1:
                st.subheader('Overbought Symbols (RSI > 70)')
                if not df_above_70.empty:
                    st.dataframe(df_above_70.style.format({'15m RSI': '{:.2f}', 'RSI Change': '{:.2f}%'}))
                    
                    # Bar chart for overbought symbols
                    fig_overbought = px.bar(df_above_70, x='Symbol', y='15m RSI', title='Overbought Symbols (RSI > 70)')
                    st.plotly_chart(fig_overbought, use_container_width=True)
                else:
                    st.write("No symbols with RSI above 70.")

                st.subheader('Symbols with Rising RSI (45-55)')
                if not df_rising.empty:
                    st.dataframe(df_rising.style.format({'15m RSI': '{:.2f}', 'RSI Change': '{:.2f}%'}))
                else:
                    st.write("No symbols with rising RSI between 45 and 55.")

            with col2:
                st.subheader('Oversold Symbols (RSI < 30)')
                if not df_below_30.empty:
                    st.dataframe(df_below_30.style.format({'15m RSI': '{:.2f}', 'RSI Change': '{:.2f}%'}))
                    
                    # Bar chart for oversold symbols
                    fig_oversold = px.bar(df_below_30, x='Symbol', y='15m RSI', title='Oversold Symbols (RSI < 30)')
                    st.plotly_chart(fig_oversold, use_container_width=True)
                else:
                    st.write("No symbols with RSI below 30.")

                st.subheader('Symbols with Decreasing RSI (45-55)')
                if not df_decreasing.empty:
                    st.dataframe(df_decreasing.style.format({'15m RSI': '{:.2f}', 'RSI Change': '{:.2f}%'}))
                else:
                    st.write("No symbols with decreasing RSI between 45 and 55.")

            # Scatter plot for all symbols (using latest data)
            st.subheader('RSI Distribution for All Symbols')
            fig_scatter = px.scatter(latest_df, x='Symbol', y='15m RSI', color='RSI Change',
                                     color_continuous_scale='RdYlGn', hover_data=['RSI Change'],
                                     title='RSI Distribution for All Symbols')
            fig_scatter.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)")
            fig_scatter.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)")
            st.plotly_chart(fig_scatter, use_container_width=True)

        except Exception as e:
            st.error(f"An error occurred while processing the data: {str(e)}")
            st.error("Please check your CSV file for any inconsistencies in the data format.")
    else:
        st.write("No data available. Please make sure the CSV file exists and contains valid data.")

if __name__ == "__main__":
    # Start the CSV update process in a separate thread
    update_thread = threading.Thread(target=update_csv, daemon=True)
    update_thread.start()
    
    # Run the Streamlit app
    display_streamlit_app()