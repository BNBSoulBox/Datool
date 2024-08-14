import discord
from discord.ext import tasks
import pandas as pd
import os
from cachetools import TTLCache
import plotly.express as px
import io
from datetime import datetime, timezone
import asyncio
import platform

# Discord bot token
DISCORD_BOT_TOKEN = 'MTI3MTE4OTY5MzE2NTM0Mjc1Ng.Gwikl5.2QfVdf1ONOQVSzfVAia-k7Fa3kLO3agZ6RwKf4'

# Discord channel ID
CHANNEL_ID = 1262432301569278023

# Path to the CSV file
CSV_FILE_PATH = "15m_rsi.csv"

# Cache setup
cache = TTLCache(maxsize=1000, ttl=1800)  # 30-minute TTL

# Create Discord client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

def load_and_process_data():
    df = pd.read_csv(CSV_FILE_PATH)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', utc=True)
    df = df.dropna(subset=['Timestamp'])
    df['15m RSI'] = pd.to_numeric(df['15m RSI'], errors='coerce')
    return df

def get_latest_data(df):
    latest_timestamp = df['Timestamp'].max()
    latest_df = df[df['Timestamp'] == latest_timestamp].copy()
    return latest_df, latest_timestamp

def create_scatter_plot(df, selected_symbols):
    fig = px.scatter(df, x='Symbol', y='15m RSI', color='15m RSI',
                     color_continuous_scale='RdYlGn',
                     title='RSI Distribution for Selected Symbols')
    fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Overbought (70)")
    fig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Oversold (30)")
    
    # Highlight selected symbols
    for symbol in selected_symbols:
        symbol_data = df[df['Symbol'] == symbol]
        fig.add_trace(px.scatter(symbol_data, x='Symbol', y='15m RSI', text='Symbol').data[0])
    
    img_bytes = fig.to_image(format="png")
    return io.BytesIO(img_bytes)

@tasks.loop(minutes=30)
async def send_alerts():
    try:
        df = load_and_process_data()
        latest_df, latest_timestamp = get_latest_data(df)
        
        overbought_df = latest_df[latest_df['15m RSI'] > 70].sort_values(by='15m RSI', ascending=False)
        oversold_df = latest_df[latest_df['15m RSI'] < 30].sort_values(by='15m RSI', ascending=True)
        
        embed = discord.Embed(title="RSI Alert", color=0x00ff00)
        embed.set_thumbnail(url="https://example.com/your_logo.png")  # Replace with your logo URL
        
        if not overbought_df.empty:
            overbought_list = [f"{row['Symbol']} (Power: {row['15m RSI']:.2f})" for _, row in overbought_df.iterrows()]
            embed.add_field(name="🚀 Take Long (Overbought)", value="\n".join(overbought_list), inline=False)
        
        if not oversold_df.empty:
            oversold_list = [f"{row['Symbol']} (Power: {row['15m RSI']:.2f})" for _, row in oversold_df.iterrows()]
            embed.add_field(name="🔻 Take Short (Oversold)", value="\n".join(oversold_list), inline=False)
        
        embed.set_footer(text=f"Last Update: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        channel = client.get_channel(CHANNEL_ID)
        if channel is None:
            print(f"Error: Could not find channel with ID {CHANNEL_ID}. Make sure the bot has access to this channel.")
            return
        
        # Create and send scatter plot
        selected_symbols = list(overbought_df['Symbol']) + list(oversold_df['Symbol'])
        plot_img = create_scatter_plot(latest_df, selected_symbols)
        plot_file = discord.File(plot_img, filename="rsi_scatter.png")
        
        await channel.send(embed=embed, file=plot_file)
        print(f"Alert sent successfully at {datetime.now()}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

@client.event
async def on_ready():
    print(f'{client.user} has connected to Discord!')
    print(f"Bot is in the following guilds:")
    for guild in client.guilds:
        print(f"- {guild.name} (id: {guild.id})")
        for channel in guild.text_channels:
            print(f"  - #{channel.name} (id: {channel.id})")
    send_alerts.start()

def run_bot():
    try:
        asyncio.run(client.start(DISCORD_BOT_TOKEN))
    except KeyboardInterrupt:
        print("Bot is shutting down...")

if __name__ == "__main__":
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run_bot()