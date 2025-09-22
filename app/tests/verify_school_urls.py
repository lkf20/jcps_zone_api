# app/tests/verify_school_urls.py

import sqlite3
import os
import requests
import concurrent.futures

# --- Configuration ---
# Assumes this script is run from the root of the `jcps_gis_api` project.
DATABASE_PATH = os.path.join('app', 'jcps_school_data.db')
TABLE_NAME = 'schools'

# URL columns to check in the database
URL_COLUMNS = ['school_website_link', 'ky_reportcard_URL']

# --- Request Configuration ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
TIMEOUT = 10
MAX_WORKERS = 15


def get_urls_from_db():
    """Fetches all school URLs from the database."""
    if not os.path.exists(DATABASE_PATH):
        print(f"❌ FATAL ERROR: Database not found at '{DATABASE_PATH}'.")
        print("   Please run this script from the root directory of the 'jcps_gis_api' project.")
        return None

    urls_to_check = []
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        columns_str = ", ".join(f'"{col}"' for col in URL_COLUMNS)
        query = f'SELECT display_name, {columns_str} FROM "{TABLE_NAME}"'
        
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            school_name = row['display_name']
            for col_name in URL_COLUMNS:
                url = row[col_name]
                if url and url.strip():
                    urls_to_check.append((school_name, col_name, url.strip()))
        
        conn.close()
        return urls_to_check

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return None

def check_url(school_name, url_type, url):
    """
    Checks a single URL and returns its status.
    Returns a tuple: (status, school_name, url_type, url, message)
    """
    try:
        response = requests.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return ('OK', school_name, url_type, url, f"OK - Status {response.status_code}")

    except requests.exceptions.HTTPError as e:
        return ('BROKEN', school_name, url_type, url, f"HTTP Error - Status {e.response.status_code}")
    except requests.exceptions.Timeout:
        return ('BROKEN', school_name, url_type, url, "Error - Timed out")
    except requests.exceptions.RequestException as e:
        return ('BROKEN', school_name, url_type, url, f"Connection Error - {type(e).__name__}")


if __name__ == "__main__":
    print("--- Starting School URL Verification Script ---")
    
    urls_to_verify = get_urls_from_db()
    
    if urls_to_verify is None:
        exit(1)

    total_urls = len(urls_to_verify)
    print(f"🔎 Found {total_urls} URLs to check. Starting verification...")

    broken_links = []
    good_links_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(check_url, name, type, url): (name, type, url) for name, type, url in urls_to_verify}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
            try:
                status, s_name, u_type, url, msg = future.result()
                if status == 'BROKEN':
                    broken_links.append({'name': s_name, 'type': u_type, 'url': url, 'error': msg})
                else:
                    good_links_count += 1
                
                print(f"\rProgress: {i + 1}/{total_urls}", end="", flush=True)

            except Exception as exc:
                name, type, url = future_to_url[future]
                broken_links.append({'name': name, 'type': type, 'url': url, 'error': f"Script Exception: {exc}"})

    print("\n\n--- ✅ Verification Complete ---")
    print("\n--- Summary ---")
    print(f"Total URLs Checked: {total_urls}")
    print(f"  - ✅ Good URLs: {good_links_count}")
    print(f"  - ❌ Broken URLs: {len(broken_links)}")
    
    # <<< START: MODIFIED OUTPUT SECTION >>>
    if broken_links:
        print("\n--- 🚨 Broken Links (Copy and Paste the CSV data below) ---")
        print("School Name,Broken URL") # CSV Header
        # Sort for consistent reporting
        broken_links.sort(key=lambda x: x['name'])
        for link in broken_links:
            # Print in CSV format (School Name, URL) for easy pasting
            print(f"\"{link['name']}\",\"{link['url']}\"")
    else:
        print("\n🎉 All URLs verified successfully!")
    # <<< END: MODIFIED OUTPUT SECTION >>>