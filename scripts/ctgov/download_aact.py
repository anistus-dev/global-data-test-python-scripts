import os
import sys
import requests
import argparse
from datetime import datetime

def download_aact(target_date=None, output_dir="data/aact_dumps"):
    """
    Download the AACT daily static database copy for a specific date.
    """
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"aact_dump_{target_date}.zip"
    output_path = os.path.join(output_dir, filename)
    
    # AACT Daily Dump URL pattern
    url = f"https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/{target_date}?source=web"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"Targeting AACT Dump for date: {target_date}")
    print(f"URL: {url}")
    
    try:
        # Stream the download to handle large files
        response = requests.get(url, headers=headers, stream=True, timeout=60)
        
        # Check if the file actually exists (AACT often redirects to a 404 page if date is invalid)
        if response.status_code != 200:
            print(f"Error: Could not find dump for {target_date} (Status Code: {response.status_code})")
            print("Note: AACT daily dumps are typically available for the current date or recent past.")
            return False

        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024 * 1024 # 1MB
        
        print(f"Saving to: {output_path}")
        if total_size > 0:
            print(f"Total size: {total_size / (1024*1024):.2f} MB")
        else:
            print("Total size: Unknown (Streaming...)")

        downloaded = 0
        with open(output_path, 'wb') as f:
            for data in response.iter_content(block_size):
                f.write(data)
                downloaded += len(data)
                
                if total_size > 0:
                    done = int(50 * downloaded / total_size)
                    percent = (downloaded / total_size) * 100
                    sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {percent:3.1f}% ({downloaded / (1024*1024):.1f} MB)")
                else:
                    sys.stdout.write(f"\rDownloaded: {downloaded / (1024*1024):.1f} MB...")
                sys.stdout.flush()

        print("\n\nDownload complete!")
        return True

    except requests.exceptions.RequestException as e:
        print(f"\nConnection error occurred: {e}")
        return False
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Download AACT daily static database copies.")
    parser.add_argument(
        "--date", 
        help="Date of the dump in YYYY-MM-DD format (default: today)",
        default=None
    )
    parser.add_argument(
        "--out", 
        help="Output directory for the downloaded file",
        default="data/aact_dumps"
    )

    args = parser.parse_args()
    
    success = download_aact(target_date=args.date, output_dir=args.out)
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
