import os
import sys
import argparse
from playwright.sync_api import sync_playwright

def download_isrctn_csv(output_path="data/isrctn_ids.csv"):
    """
    Download the ISRCTN trial ID list using Headless Playwright.
    Bypasses JS challenges by using a real browser engine in the background.
    """
    url = "https://www.isrctn.com/searchCsv?q=&columns=ISRCTN"
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Attempting to download ISRCTN CSV using Headless Browser...")
    
    try:
        with sync_playwright() as p:
            # Launch headless browser
            browser = p.chromium.launch(headless=True)
            
            # Create a new page with a realistic user agent
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            print(f"Navigating to: {url}")
            
            # Start waiting for the download event before clicking or navigating
            with page.expect_download() as download_info:
                page.goto(url, wait_until="networkidle")
            
            download = download_info.value
            
            # Save the file
            download.save_as(output_path)
            
            print(f"Success! CSV saved to: {output_path}")
            
            # Verify and count
            if os.path.exists(output_path):
                with open(output_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if len(lines) > 1:
                        print(f"Found {len(lines) - 1} trial IDs.")
            
            browser.close()
            return True

    except Exception as e:
        print(f"\nAn error occurred during browser automation: {e}")
        print("\nNote: If this is your first time running, ensure you have installed the browser and system dependencies:")
        print("uv run playwright install chromium")
        print("uv run playwright install-deps chromium")
        return False

def main():
    parser = argparse.ArgumentParser(description="Download the latest list of ISRCTN trial IDs using Headless Playwright.")
    parser.add_argument(
        "--out", 
        help="Path to save the downloaded CSV",
        default="data/isrctn_ids.csv"
    )

    args = parser.parse_args()
    
    success = download_isrctn_csv(output_path=args.out)
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
