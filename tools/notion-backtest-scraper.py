import time
import os
import pandas as pd
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

try:
    import requests
except ImportError:
    print("Error: The 'requests' library is not installed.")
    print("Please install it by running: pip install requests")
    exit()

# --- Configuration ---
URL = "https://blinchikof.notion.site/Silver-Bullet-Backtest-7ba22a3e700b41eeaafd017b82915d16"
OUTPUT_DIR = "output"
IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "notion_data.csv")
DEBUG_FILE = "notion_page_source.html"

# --- Main Script ---
def setup_driver():
    """Sets up the Chrome driver."""
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def get_image_url(soup):
    """Extracts and decodes the image URL from the side panel soup."""
    peek_renderer = soup.find('div', class_='notion-peek-renderer')
    if not peek_renderer:
        print("  - Image Error: Peek renderer not found.")
        return None

    img_tag = peek_renderer.find('img')
    if not (img_tag and img_tag.get('src')):
        print("  - Image Error: Image tag or src attribute not found.")
        return None

    raw_src = img_tag.get('src')
    print(f"  - Found raw image src: {raw_src[:100]}...")

    if 'http' in raw_src:
        return raw_src.split('?')[0]
    if raw_src.startswith('/image/'):
        try:
            encoded_url = raw_src.split('/image/')[1]
            return urllib.parse.unquote(encoded_url.split('?')[0])
        except Exception as e:
            print(f"  - Image Error: Could not parse encoded URL. {e}")
            return None
    
    return raw_src

def download_image(image_url, asset_name, index):
    """Downloads an image from a URL and saves it locally."""
    if not image_url:
        return "No URL found"

    try:
        sanitized_name = "".join(filter(str.isalnum, asset_name))[:20]
        filename = f"{index+1:02d}_{sanitized_name}.png"
        
        if not os.path.exists(IMAGES_DIR):
            os.makedirs(IMAGES_DIR)

        image_path = os.path.join(IMAGES_DIR, filename)

        print(f"  - Downloading image to {image_path}...")
        response = requests.get(image_url, timeout=15)
        response.raise_for_status()
        
        with open(image_path, 'wb') as f:
            f.write(response.content)
        print(f"  - Successfully saved image.")
        return image_path

    except requests.exceptions.RequestException as e:
        print(f"  - Error downloading image: {e}")
        return "Download failed"

def main():
    driver = setup_driver()
    data = []

    try:
        print(f"Opening Notion page: {URL}")
        driver.get(URL)

        print("Waiting for table rows to load...")
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "notion-table-view-row"))
            )
            print("Table rows found.")
            time.sleep(3)
        except Exception as e:
            print(f"Fatal: Could not find table rows. Error: {e}")
            with open(DEBUG_FILE, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print(f"Saved page source to {DEBUG_FILE} for debugging.")
            return

        total_rows = len(driver.find_elements(By.CLASS_NAME, 'notion-table-view-row'))
        print(f"Found {total_rows} rows to process.")

        for i in range(total_rows):
            image_path = "Not processed"
            try:
                rows = driver.find_elements(By.CLASS_NAME, 'notion-table-view-row')
                if i >= len(rows):
                    print(f"Warning: Row index {i} is out of bounds. Stopping.")
                    break
                
                row = rows[i]
                
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", row)
                time.sleep(0.5)

                cells = row.find_elements(By.CLASS_NAME, 'notion-table-view-cell')
                if len(cells) <= 4:
                    print(f"Warning: Skipping row {i+1} due to insufficient cells ({len(cells)}).")
                    continue

                asset_name = cells[0].text.strip() or "Empty"
                result = cells[4].text.strip() or "Empty"
                print(f"\nProcessing row {i+1}/{total_rows}: Asset='{asset_name}', Result='{result}'")

                cells[0].click()
                
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".notion-peek-renderer img"))
                )
                print("  - Side panel with image appeared.")
                time.sleep(1)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                image_url = get_image_url(soup)
                image_path = download_image(image_url, asset_name, i)
                
            except Exception as e:
                print(f"  - Error processing row {i+1}. Skipping. Details: {e}")
                image_path = "Processing error"
            finally:
                data.append({'Asset Name': asset_name, 'Result': result, 'Image Path': image_path})
                try:
                    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                    time.sleep(0.5)
                except:
                    pass
    
    finally:
        driver.quit()
        print("\nBrowser closed.")

    if data:
        df = pd.DataFrame(data)
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\nData successfully parsed and saved to {OUTPUT_FILE}")
        print("--- First 5 rows ---")
        print(df.head())
    else:
        print("\nNo data was parsed. Please check the script and website.")

if __name__== "__main__":
    main() 