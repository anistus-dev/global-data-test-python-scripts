import os
import sys
import requests
import argparse
import zipfile
import shutil
from datetime import datetime

def download_aact(target_date=None, output_dir="data/aact_dumps"):
    """
    Download and extract the AACT daily static database copy for a specific date.
    """
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    # Create a dated subfolder for this specific dump
    dated_dir = os.path.join(output_dir, target_date)
    os.makedirs(dated_dir, exist_ok=True)
    
    zip_filename = f"aact_dump_{target_date}.zip"
    zip_path = os.path.join(dated_dir, zip_filename)
    
    # AACT Daily Dump URL pattern
    url = f"https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/{target_date}?source=web"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"Targeting AACT Dump for date: {target_date}")
    print(f"URL: {url}")
    
    try:
        # 1. Download the ZIP file
        response = requests.get(url, headers=headers, stream=True, timeout=60)
        
        # Check if the file actually exists (AACT often redirects to a 404 page if date is invalid)
        if response.status_code != 200:
            print(f"Error: Could not find dump for {target_date} (Status Code: {response.status_code})")
            print("Note: AACT daily dumps are typically available for the current date or recent past.")
            return False

        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024 * 1024 # 1MB
        
        print(f"Downloading to: {zip_path}")
        if total_size > 0:
            print(f"Total size: {total_size / (1024*1024):.2f} MB")
        else:
            print("Total size: Unknown (Streaming...)")

        downloaded = 0
        with open(zip_path, 'wb') as f:
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

        print("\nDownload complete!")

        # 2. Extract the ZIP file
        print(f"Extracting files into {dated_dir}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dated_dir)
        
        # 3. Cleanup: Find the .dmp file and remove everything else
        print("Cleaning up temporary files...")
        dmp_file = None
        
        # Walk through the extracted files
        for root, dirs, files in os.walk(dated_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith('.dmp'):
                    # If the dmp is in a subdirectory, move it to the dated_dir root
                    target_path = os.path.join(dated_dir, file)
                    if file_path != target_path:
                        shutil.move(file_path, target_path)
                    dmp_file = target_path
                elif file == zip_filename:
                    # Keep the zip for a moment, delete later
                    pass
                else:
                    # Delete non-dmp files (READMEs, etc.)
                    os.remove(file_path)

        # Remove the original ZIP file
        if os.path.exists(zip_path):
            os.remove(zip_path)

        # Final check
        if dmp_file:
            print(f"\nSuccess! Dump is ready at: {dmp_file}")
            print(f"You can now run: uv run python -m scripts.ctgov.init_db {dmp_file}")
            return True
        else:
            print("\nWarning: No .dmp file was found inside the downloaded ZIP.")
            return False

    except zipfile.BadZipFile:
        print("\nError: The downloaded file is not a valid ZIP file. AACT might have returned a 404 page.")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return False
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Download and extract AACT daily database copies.")
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
