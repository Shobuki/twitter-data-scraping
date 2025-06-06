import asyncio
import pandas as pd
from twikit import Client
from datetime import datetime, timedelta, timezone
import os
import sys

ACCOUNTS = [
    {"USERNAME": "", "EMAIL": "", "PASSWORD": ""},
    {"USERNAME": "", "EMAIL": "", "PASSWORD": ""},
    {"USERNAME": "", "EMAIL": "", "PASSWORD": ""},
    {"USERNAME": "", "EMAIL": "", "PASSWORD": ""},
]

START_DATE = datetime(2017, 9, 1, tzinfo=timezone.utc)
END_DATE = datetime(2018, 12, 31, tzinfo=timezone.utc)
total_days = (END_DATE.date() - START_DATE.date()).days + 1
DATES = [
    (START_DATE + timedelta(days=i), START_DATE + timedelta(days=i, hours=23, minutes=59, seconds=59))
    for i in range(total_days)
]

def split_dates(dates, n):
    groups = [[] for _ in range(n)]
    for i, date in enumerate(dates):
        groups[i % n].append((i, date))
    return groups

DATES_SPLIT = split_dates(DATES, len(ACCOUNTS))

QUERY = '(#MeToo) ("sexual harassment" OR molest OR rape OR assault OR abuse OR abused OR harassment OR sexually OR grooming OR predator OR consent OR inappropriate OR harasser OR violated OR unwanted OR misconduct OR groped OR "alyssa milano")'
RESULTS_PER_PAGE = 20
MAX_PER_DAY = 24
BATCH_SIZE = 500
OUTPUT_FILE = 'all_tweets_raw.xlsx'
FINAL_FILE = 'all_tweets.xlsx'
DELAY_WRITE = 60

class Fatal404(Exception): pass

class GlobalCounter:
    def __init__(self):
        self.total = 0
        self.lock = asyncio.Lock()
    async def add(self, n):
        async with self.lock:
            self.total += n
    async def get(self):
        async with self.lock:
            return self.total

class BatchWriter:
    def __init__(self, batch_size, output_file, delay_write):
        self.lock = asyncio.Lock()
        self.batch = []
        self.batch_size = batch_size
        self.output_file = output_file
        self.delay_write = delay_write
        self.last_write_time = None

        # Inisialisasi file baru dengan header kolom, jika belum ada
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        df_init = pd.DataFrame(columns=[
            "username", "text", "jumlah repost", "jumlah like", "jumlah komen", "tanggal tweet", "link"
        ])
        try:
            with pd.ExcelWriter(self.output_file, engine='openpyxl', mode='w') as writer:
                df_init.to_excel(writer, index=False)
        except Exception as e:
            print(f"[ERROR] Gagal inisialisasi file Excel: {e}")
            sys.exit(1)

    async def add_data(self, new_data):
        async with self.lock:
            self.batch.extend(new_data)
            if len(self.batch) >= self.batch_size:
                await self.flush()

    async def flush(self):
        if not self.batch:
            return
        print(f"[WRITER] Writing {len(self.batch)} tweets to {self.output_file} ...")
        try:
            # Load existing file (kecuali header)
            if os.path.exists(self.output_file):
                old_df = pd.read_excel(self.output_file)
            else:
                old_df = pd.DataFrame(columns=[
                    "username", "text", "jumlah repost", "jumlah like", "jumlah komen", "tanggal tweet", "link"
                ])
            # Gabungkan data lama dengan batch baru
            df_new = pd.DataFrame(self.batch)
            df_new["tanggal tweet"] = pd.to_datetime(df_new["tanggal tweet"], errors='coerce')
            all_df = pd.concat([old_df, df_new], ignore_index=True)
            all_df = all_df.sort_values(by="tanggal tweet", ascending=True)
            all_df = all_df.drop_duplicates(subset=["link"]) # Hindari duplikasi link
            # Tulis ulang seluruh file dengan header
            with pd.ExcelWriter(self.output_file, engine='openpyxl', mode='w') as writer:
                all_df.to_excel(writer, index=False)
        except Exception as e:
            print(f"[ERROR] Failed to write to Excel: {e}")
            sys.exit(1)
        print(f"[WRITER] Batch written. Sleep {self.delay_write} seconds (all workers pause).")
        self.batch = []
        self.last_write_time = datetime.now()
        await asyncio.sleep(self.delay_write)

async def retry_with_backoff(async_func, *args, worker_tag="", global_counter=None, worker_total=None, **kwargs):
    DELAY_RETRY_20S = 20
    MAX_TRY_20S = 5
    DELAY_1M = 60
    DELAY_5M = 5 * 60
    DELAY_15M = 15 * 60
    MAX_15M_REPEAT = 3

    for attempt in range(1, MAX_TRY_20S+1):
        try:
            return await async_func(*args, **kwargs)
        except Exception as e:
            if 'code":34' in str(e) or 'page does not exist' in str(e):
                print(f"{worker_tag}[FATAL] 404 Not Found. Seluruh proses akan di-restart.")
                raise Fatal404(str(e))
            if '429' in str(e) or 'Rate limit' in str(e):
                gtotal = await global_counter.get() if global_counter else "-"
                print(f"{worker_tag}[LIMIT/429] Rate limit! Worker total: {worker_total[0]} | Global total: {gtotal}")
            print(f"{worker_tag}[RETRY-20s] Gagal (ke-{attempt}/{MAX_TRY_20S}): {e}")
            if attempt < MAX_TRY_20S:
                await asyncio.sleep(DELAY_RETRY_20S)
    print(f"{worker_tag}[RETRY-1m] Coba lagi setelah 1 menit ...")
    await asyncio.sleep(DELAY_1M)
    try:
        return await async_func(*args, **kwargs)
    except Exception as e:
        if 'code":34' in str(e) or 'page does not exist' in str(e):
            print(f"{worker_tag}[FATAL] 404 Not Found. Seluruh proses akan di-restart.")
            raise Fatal404(str(e))
        if '429' in str(e) or 'Rate limit' in str(e):
            gtotal = await global_counter.get() if global_counter else "-"
            print(f"{worker_tag}[LIMIT/429] Rate limit! Worker total: {worker_total[0]} | Global total: {gtotal}")
        print(f"{worker_tag}[RETRY-1m] Gagal: {e}")
    print(f"{worker_tag}[RETRY-5m] Coba lagi setelah 5 menit ...")
    await asyncio.sleep(DELAY_5M)
    try:
        return await async_func(*args, **kwargs)
    except Exception as e:
        if 'code":34' in str(e) or 'page does not exist' in str(e):
            print(f"{worker_tag}[FATAL] 404 Not Found. Seluruh proses akan di-restart.")
            raise Fatal404(str(e))
        if '429' in str(e) or 'Rate limit' in str(e):
            gtotal = await global_counter.get() if global_counter else "-"
            print(f"{worker_tag}[LIMIT/429] Rate limit! Worker total: {worker_total[0]} | Global total: {gtotal}")
        print(f"{worker_tag}[RETRY-5m] Gagal: {e}")
    for repeat15 in range(1, MAX_15M_REPEAT+1):
        print(f"{worker_tag}[RETRY-15m] Coba ke-{repeat15}/{MAX_15M_REPEAT} setelah 15 menit ...")
        await asyncio.sleep(DELAY_15M)
        try:
            return await async_func(*args, **kwargs)
        except Exception as e:
            if 'code":34' in str(e) or 'page does not exist' in str(e):
                print(f"{worker_tag}[FATAL] 404 Not Found. Seluruh proses akan di-restart.")
                raise Fatal404(str(e))
            if '429' in str(e) or 'Rate limit' in str(e):
                gtotal = await global_counter.get() if global_counter else "-"
                print(f"{worker_tag}[LIMIT/429] Rate limit! Worker total: {worker_total[0]} | Global total: {gtotal}")
            print(f"{worker_tag}[RETRY-15m] Gagal ke-{repeat15}: {e}")
    print(f"{worker_tag}[END] Semua percobaan gagal.")
    return None

async def scrape_account(idx, account, assigned_dates, global_counter, batch_writer):
    worker_tag = f"[WORKER-{idx+1}|{account['USERNAME']}] "
    print(f"\n{worker_tag}Inisialisasi client ...")
    client = Client('en-US')
    cookies_file = f'cookies{idx}.json'
    worker_total = [0]
    try:
        print(f"{worker_tag}Mencoba load session dari cookies ...")
        client.load_cookies(cookies_file)
        user = await client.user()
        print(f"{worker_tag}Sukses pakai cookies. Login sebagai: {user.screen_name}")
    except Exception as e:
        print(f"{worker_tag}Cookies gagal dipakai atau expired: {e}")
        print(f"{worker_tag}Login manual dan update cookies ...")
        await retry_with_backoff(
            client.login,
            auth_info_1=account["USERNAME"],
            auth_info_2=account["EMAIL"],
            password=account["PASSWORD"],
            cookies_file=cookies_file,
            worker_tag=worker_tag,
            global_counter=global_counter,
            worker_total=worker_total
        )
        user = await retry_with_backoff(
            client.user,
            worker_tag=worker_tag,
            global_counter=global_counter,
            worker_total=worker_total
        )
        print(f"{worker_tag}Login berhasil, cookies diperbarui. Login sebagai: {user.screen_name}")

    for day_index, (start_date, end_date) in assigned_dates:
        q = f"{QUERY} lang:en since:{start_date.strftime('%Y-%m-%d')} until:{(end_date + timedelta(seconds=1)).strftime('%Y-%m-%d')}"
        next_result = await retry_with_backoff(
            client.search_tweet, q, product='Latest', count=RESULTS_PER_PAGE,
            worker_tag=worker_tag, global_counter=global_counter, worker_total=worker_total
        )
        tweets_data = []
        count = 0
        page = 1

        while next_result and count < MAX_PER_DAY:
            for tweet in next_result:
                tgl = tweet.created_at_datetime
                if not (start_date <= tgl <= end_date):
                    continue
                tweets_data.append({
                    "username": tweet.user.screen_name,
                    "text": tweet.full_text,
                    "jumlah repost": tweet.retweet_count,
                    "jumlah like": tweet.favorite_count,
                    "jumlah komen": tweet.reply_count,
                    "tanggal tweet": tgl.strftime("%Y-%m-%d %H:%M:%S"),
                    "link": f"https://twitter.com/{tweet.user.screen_name}/status/{tweet.id}",
                })
                count += 1
                worker_total[0] += 1
                await global_counter.add(1)
                if count >= MAX_PER_DAY:
                    break
            if count >= MAX_PER_DAY:
                break
            if not getattr(next_result, "next_cursor", None):
                break
            next_result = await retry_with_backoff(
                next_result.next,
                worker_tag=worker_tag,
                global_counter=global_counter,
                worker_total=worker_total
            )
            page += 1

        if tweets_data:
            await batch_writer.add_data(tweets_data)
        print(f"{worker_tag}{start_date.strftime('%Y-%m-%d')}: {count} tweet, Worker total: {worker_total[0]}, Global total: {await global_counter.get()}")

    await batch_writer.flush()
    print(f"{worker_tag}Scraping selesai: Worker total: {worker_total[0]}, Global total: {await global_counter.get()}")
    return worker_total[0]

async def main():
    global_counter = GlobalCounter()
    batch_writer = BatchWriter(BATCH_SIZE, OUTPUT_FILE, DELAY_WRITE)
    try:
        tasks = []
        for idx, acc in enumerate(ACCOUNTS):
            assigned_dates = DATES_SPLIT[idx]
            tasks.append(scrape_account(idx, acc, assigned_dates, global_counter, batch_writer))
        results = await asyncio.gather(*tasks)
        # Final flush after all workers finish
        await batch_writer.flush()
        for i, total in enumerate(results):
             print(f"[SUMMARY] Worker-{i+1} ({ACCOUNTS[i]['USERNAME']}): {total} tweet")
        print(f"[SUMMARY] Total seluruh worker: {sum(results)} tweet")

        print("\n[POST-PROCESS] Merging, sorting and removing duplicates...")

        # Read all written raw data, drop duplicates, sort, then re-write to final file
        if os.path.exists(OUTPUT_FILE):
            df = pd.read_excel(OUTPUT_FILE)
            df = df.drop_duplicates(subset=['link'])
            df["tanggal tweet"] = pd.to_datetime(df["tanggal tweet"], errors="coerce")  # Tambahkan ini
            df = df.sort_values(by="tanggal tweet", ascending=True)
            df.to_excel(FINAL_FILE, index=False)
            print(f"[POST-PROCESS] Final data written to {FINAL_FILE} (total rows: {len(df)})")
        else:
            print("[POST-PROCESS] No raw data to merge.")
        print(f"\n✅ Semua worker selesai. Jumlah tweet per worker: {results}")
        print(f"✅ Total semua worker: {await global_counter.get()}")
    except Fatal404:
        print("\n[FATAL] Terjadi error 404 pada salah satu akun. Semua proses akan diulang (restart)...\n")
        await asyncio.sleep(5)
        os.execv(sys.executable, [sys.executable] + sys.argv)

if __name__ == "__main__":
    asyncio.run(main())
