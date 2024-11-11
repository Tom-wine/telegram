import asyncio
import re
import csv
import aiohttp
from datetime import timezone, datetime, timedelta
from telethon import TelegramClient, events
import os

api_id = ''
api_hash = ''
phone_number = ''

telegram_client = TelegramClient('session_name', api_id, api_hash)

configurations = []
csv_file_path = r""
webhook_mapping = {}
user_id_mapping = {}
occurrence_check_mapping = {}
blacklist_user_mapping = {}

with open(csv_file_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    print(f"CSV Headers: {reader.fieldnames}")
    for row in reader:
        print(f"Row: {row}")
        configurations.append(row)
        telegram_account = row['telegram_account']
        webhook_mapping[telegram_account] = row['discord_webhook']
        user_id_mapping[telegram_account] = row['user_ID'].split(',') if row['user_ID'] else []
        occurrence_check_mapping[telegram_account] = row.get('occurrence_check', 'false').lower() == 'true'
        blacklist_user_mapping[telegram_account] = row['blacklist_user'].split(',') if row['blacklist_user'] else []

contract_address_pattern = re.compile(r'\b[A-Za-z0-9]{40,50}\b')
dexscreener_link_pattern = re.compile(r'https://dexscreener\.com/[A-Za-z0-9]+/[A-Za-z0-9]+')
bullxio_link_pattern = re.compile(r'https://bullx.io/terminal\?chainId=1399811149&address=[A-Za-z0-9]+')
pumpfun_link_pattern = re.compile(r'https://pump.fun/[A-Za-z0-9]+')
ticker_pattern = re.compile(r'\$[A-Za-z]+')
birdeye_link_pattern = re.compile(r'https://birdeye.so/token/[A-Za-z0-9]+/[A-Za-z0-9]+?chain=solana')

TIME_WINDOW_MINUTES = 20

log_csv_path = r"C:\Users\Administrator\Documents\ProjectAlpha\Telegrammodules\LoggedAddresses.csv"
fieldnames = ['timestamp', 'group', 'contract_address', 'ticker']


file_lock = asyncio.Lock()


if not os.path.exists(log_csv_path) or os.path.getsize(log_csv_path) == 0:
    with open(log_csv_path, 'w', newline='') as logfile:
        writer = csv.DictWriter(logfile, fieldnames=fieldnames)
        writer.writeheader()

last_scan_times = {}
last_log_times = {}  
sent_messages = set()  

async def send_to_discord(webhook_url, content=None, embed=None):
    async with aiohttp.ClientSession() as session:
        payload = {}
        if content:
            payload['content'] = content
        if embed:
            payload['embeds'] = [embed]
        async with session.post(webhook_url, json=payload) as response:
            if response.status == 429:
                retry_after = int(response.headers.get("Retry-After", 1))
                await asyncio.sleep(retry_after)
                await send_to_discord(webhook_url, content, embed)

def clean_message(text):
    text = text.replace("Major Launch Radar", "Alpha Project Radar")
    text = text.replace("Called by GaryGemsler", "Alpha Project Radar")
    return text.strip()

def extract_links_and_addresses(text):
    addresses = contract_address_pattern.findall(text)
    tickers = ticker_pattern.findall(text)
    return addresses, tickers

async def log_to_csv(timestamp, group, contract_address, ticker):
    async with file_lock:
        with open(log_csv_path, 'a', newline='') as logfile:
            writer = csv.DictWriter(logfile, fieldnames=fieldnames)
            writer.writerow({'timestamp': timestamp, 'group': group, 'contract_address': contract_address, 'ticker': ticker})

def read_log_entries():
    log_entries = []
    with open(log_csv_path, newline='') as logfile:
        reader = csv.DictReader(logfile)
        for row in reader:
            if row['timestamp'].lower() == 'timestamp':
                continue
            log_entries.append(row)
    return log_entries

def parse_log_entries():
    log_entries = read_log_entries()
    now = datetime.now(timezone.utc)
    occurrences = {}

    for log_entry in log_entries:
        timestamp_str = log_entry['timestamp']
        group = log_entry['group']
        contract_address = log_entry['contract_address']
        
        try:
            timestamp = parse_timestamp(timestamp_str)
        except ValueError as e:
            print(f"Failed to parse timestamp: {timestamp_str}. Error: {e}")
            continue
        
        if (now - timestamp) > timedelta(minutes=TIME_WINDOW_MINUTES):
            continue

        if contract_address not in occurrences:
            occurrences[contract_address] = set()
        
        occurrences[contract_address].add(group)

    return occurrences

def parse_timestamp(timestamp_str):
    format_strings = [
        '%Y-%m-%d %H:%M:%S %Z',
        '%Y-%m-%d %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%H:%M',
        
    ]
    
    for format_str in format_strings:
        try:
            if format_str == '%H:%M':
                now = datetime.now(timezone.utc)
                time_only = datetime.strptime(timestamp_str, format_str).time()
                parsed_time = datetime.combine(now.date(), time_only, tzinfo=timezone.utc)
            else:
                parsed_time = datetime.strptime(timestamp_str, format_str).replace(tzinfo=timezone.utc)
            return parsed_time
        except ValueError:
            continue

    raise ValueError(f"Error handling Telegram message: time data '{timestamp_str}' does not match any known format")

sent_messages = set()  

async def handle_event(event, discord_webhook, allowed_user_ids, occurrence_check, telegram_account):
    try:
        if event.message:
            print(f"Message received: {event.message.message}")  
            message_text = event.message.message if event.message.message else ""
            message_text = clean_message(message_text)
            message_time = event.message.date.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            sender_id = str(event.sender_id)

            if allowed_user_ids and sender_id not in allowed_user_ids:
                print(f"Sender {sender_id} not in allowed user IDs for group {telegram_account}. Ignoring message.")
                return

            if sender_id in blacklist_user_mapping.get(telegram_account, []):
                print(f"Message from blacklisted user {sender_id}. Ignoring.")
                return

            addresses, tickers = extract_links_and_addresses(message_text)
            print(f"Extracted addresses: {addresses} and tickers: {tickers} from message in group {telegram_account}")

            parsed_message_time = parse_timestamp(message_time)

            for address in addresses:
                
                if address.startswith('0x'):
                    print(f"Skipping Ethereum address: {address}")
                    continue

                ticker = tickers[0] if tickers else ""

                
                last_log_time = last_log_times.get((telegram_account, address))
                if last_log_time and parsed_message_time - last_log_time < timedelta(minutes=TIME_WINDOW_MINUTES):
                    print(f"Address {address} already logged recently in group {telegram_account}. Skipping.")
                    continue

                await log_to_csv(message_time, telegram_account, address, ticker)
                last_log_times[(telegram_account, address)] = parsed_message_time  

            occurrences = parse_log_entries()

            for address in addresses:
                
                if address.startswith('0x'):
                    continue

                if address in occurrences:
                    num_groups = len(occurrences[address])
                    print(f"Address {address} found in {num_groups} groups")  

                    
                    if (address, num_groups) in sent_messages:
                        print(f"Notification for address {address} in {num_groups} groups already sent. Skipping.")
                        continue

                    if num_groups == 2:
                        await send_to_discord(
                            "https://discord.com/api/webhooks/"
                            "https://discord.com/api/webhooks/",
                            content=f"**New coin trend detected on 2 different telegram channels**"
                        )
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"!scan {address}"
                        )
                    elif num_groups == 3:
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"**New coin trend detected on 3 different telegram channels**"
                        )
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"!scan {address}"
                        )
                    elif num_groups == 5:
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"**New coin trend detected on 5 different telegram channels**"
                        )
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"!scan {address}"
                        )
                    elif num_groups == 7:
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"**New coin trend detected on 7 different telegram channels**"
                        )
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"!scan {address}"
                        )
                    elif num_groups == 10:
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"**New coin trend detected on 10 different telegram channels**"
                        )
                        await send_to_discord(
                            "https://discord.com/api/webhooks/",
                            content=f"!scan {address}"
                        )
                    
                    
                    sent_messages.add((address, num_groups))
                    print(f"Notification sent for address {address} in {num_groups} groups.")

    except Exception as e:
        print(f"Error handling Telegram message: {e}")

async def clear_log_csv_daily():
    while True:
        
        now = datetime.now()
        next_day = now + timedelta(days=1)
        next_midnight = datetime.combine(next_day, datetime.min.time())
        await asyncio.sleep((next_midnight - now).seconds)
        
        
        async with file_lock:
            with open(log_csv_path, 'w', newline='') as logfile:
                writer = csv.DictWriter(logfile, fieldnames=fieldnames)
                writer.writeheader()
        print(f"Cleared log CSV at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

message_queue = asyncio.Queue()

async def worker():
    while True:
        event, discord_webhook, allowed_user_ids, occurrence_check, telegram_account = await message_queue.get()
        await handle_event(event, discord_webhook, allowed_user_ids, occurrence_check, telegram_account)
        message_queue.task_done()

for config in configurations:
    source_chat = config['telegram_account']
    discord_webhook = config['discord_webhook']
    allowed_user_ids = user_id_mapping[source_chat]
    occurrence_check = occurrence_check_mapping[source_chat]
    telegram_account = config['telegram_account']

    try:
        if source_chat.isdigit():
            source_chat = int(source_chat)

        @telegram_client.on(events.NewMessage(chats=source_chat))
        async def telegram_handler(event, discord_webhook=discord_webhook, allowed_user_ids=allowed_user_ids, occurrence_check=occurrence_check, telegram_account=telegram_account):
            print(f"Event handler registered for {telegram_account}")
            await message_queue.put((event, discord_webhook, allowed_user_ids, occurrence_check, telegram_account))
    except ValueError:
        print(f"Invalid chat ID: {source_chat}")

async def main():
    try:
        await telegram_client.start(phone=phone_number)
        print("Telegram client started...")
        workers = [asyncio.create_task(worker()) for _ in range(3)]
        asyncio.create_task(clear_log_csv_daily())
        await telegram_client.run_until_disconnected()
        await message_queue.join()
    except Exception as e:
        print(f"Error starting Telegram client: {e}")

if __name__ == '__main__':
    asyncio.run(main())
