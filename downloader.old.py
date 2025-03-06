import requests
import concurrent.futures
from sys import argv
from urllib.parse import urlparse, unquote
import os
import re
from jsbeautifier import beautify  # Optional, for JavaScript beautification
import urllib3 

urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

def handle_errors(url):
    """Handles exceptions and prints informative error messages."""
    try:
        response = requests.get(url.strip(),verify=False)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
    except requests.exceptions.RequestException as e:
        print(f'Error downloading {url}: {e}')
        return None
    except requests.exceptions.HTTPError as e:
        print(f'Error downloading {url}: {e} ({e.response.status_code})')
        return None
    return response


def sanitize_filename(filename):
    """Removes special characters and replaces spaces with underscores."""
    return re.sub(r'[\\/:*"<>%@|]', '_', filename)


def downloader(url, output_folder):
    """Downloads content, handles potential issues, and saves."""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    sanitized_path = sanitize_filename(parsed_url.path.strip('/'))
    filename = f"{domain}_{sanitized_path}"

    if '?' in filename:
        version = filename.split('?')[-1].replace('=', '_')
        filename = version + '_' + filename.split('?')[0]

    if os.path.exists(os.path.join(output_folder, filename)):
        print(f'File already exists: {filename}')
        return

    response = handle_errors(url)
    if not response:
        return  # Skip saving if download failed

    # Check response content before saving
    if not response.text or response.status_code == 302 or response.status_code == 301:
        print(f'Skipping: {filename} (empty or redirect)')
        return


    # Beautify content if applicable (assuming JavaScript)
    # if filename.endswith('.js'):
    content = beautify(response.text)

    # Save content to the file
    with open(os.path.join(output_folder, filename), 'w', encoding='utf-8') as w:
        w.write(content)

    print(f'Downloaded: {filename}')


if __name__ == "__main__":
    try:
        input_file = argv[1]
        output_folder = argv[2]
    except IndexError:
        print('Usage: urlDownlaoder input_file output_folder')
        exit(1)

    with open(input_file) as f:
        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as e:
            [e.submit(downloader, url, output_folder) for url in f]
