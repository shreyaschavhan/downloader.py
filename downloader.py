#!/usr/bin/env python3
import os
import re
import asyncio
import logging
import argparse
import signal
import hashlib
import csv
import threading
from urllib.parse import urlparse, unquote, urlsplit, urlunsplit, quote
from typing import Set, Optional, Dict

import aiofiles
from jsbeautifier import beautify
from playwright.async_api import async_playwright, BrowserContext, Page, Response
from tqdm import tqdm
from bs4 import BeautifulSoup  # New dependency for HTML normalization

# Custom logging handler that uses tqdm.write() for non-interfering progress output.
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

# Helper function: percent-encode URL’s path and query.
def encode_url(url: str) -> str:
    parts = urlsplit(url)
    encoded_path = quote(parts.path, safe="/")
    encoded_query = quote(parts.query, safe="=&")
    return urlunsplit((parts.scheme, parts.netloc, encoded_path, encoded_query, parts.fragment))

# Helper function: remove any null bytes from URL.
def clean_url(url: str) -> str:
    return url.replace("\x00", "")

# Function to normalize HTML by removing non-critical dynamic parts.
def normalize_html(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    # Remove script and style tags, as these often include dynamic content.
    for tag in soup(["script", "style"]):
        tag.decompose()
    # Optionally, remove comments
    for comment in soup.find_all(string=lambda text: isinstance(text, type(soup.Comment))):
        comment.extract()
    # Remove extra whitespace and newlines
    normalized = ' '.join(soup.prettify().split())
    return normalized

# Global shutdown flag for graceful termination.
shutdown_flag: bool = False

# Files for processed URLs, URL mapping, and content hash tracking.
downloaded_urls_file: str = "downloaded_urls.txt"
mapping_file: str = "url_mapping.csv"
downloaded_hashes_file: str = "downloaded_hashes.txt"

downloaded_urls: Set[str] = set()
downloaded_hashes: Dict[str, str] = {}
downloaded_urls_lock = asyncio.Lock()
downloaded_hashes_lock = asyncio.Lock()
csv_lock = threading.Lock()

MAX_FILENAME_LENGTH = 255  # Maximum allowed filename length.

async def load_downloaded_urls() -> Set[str]:
    if await asyncio.to_thread(os.path.exists, downloaded_urls_file):
        async with aiofiles.open(downloaded_urls_file, "r") as f:
            lines = await f.readlines()
            return {line.strip() for line in lines if line.strip()}
    return set()

async def append_downloaded_url(url: str) -> None:
    async with downloaded_urls_lock:
        async with aiofiles.open(downloaded_urls_file, "a") as f:
            await f.write(url + "\n")
        downloaded_urls.add(url)

async def load_downloaded_hashes() -> Dict[str, str]:
    if await asyncio.to_thread(os.path.exists, downloaded_hashes_file):
        async with aiofiles.open(downloaded_hashes_file, "r") as f:
            lines = await f.readlines()
        hashes = {}
        for line in lines:
            parts = line.strip().split(",", 1)
            if len(parts) == 2:
                hashes[parts[0]] = parts[1]
        return hashes
    return {}

async def append_downloaded_hash(content_hash: str, filename: str) -> None:
    async with downloaded_hashes_lock:
        async with aiofiles.open(downloaded_hashes_file, "a") as f:
            await f.write(f"{content_hash},{filename}\n")
        downloaded_hashes[content_hash] = filename

async def append_url_mapping(short_filename: str, url: str) -> None:
    def write_csv():
        with csv_lock:
            with open(mapping_file, "a", newline='') as f:
                writer = csv.writer(f)
                writer.writerow([short_filename, url])
    await asyncio.to_thread(write_csv)

def sanitize_filename(filename: str) -> str:
    filename = unquote(filename)
    return re.sub(r'[\\/:*?"<>|]', '_', filename)

def get_extension_from_header(content_type: str) -> str:
    mapping = {
        'text/html': 'html',
        'text/plain': 'txt',
        'application/json': 'json',
        'application/javascript': 'js',
        'text/javascript': 'js',
        'text/css': 'css',
        'image/png': 'png',
        'image/jpeg': 'jpg',
        'image/gif': 'gif',
        'application/pdf': 'pdf',
        'application/zip': 'zip',
    }
    return mapping.get(content_type.split(';')[0], 'bin')

async def reset_page(page: Page) -> None:
    try:
        await page.goto("about:blank", wait_until="domcontentloaded", timeout=10000)
    except Exception as e:
        logging.warning(f"Failed to reset page state: {e}")

async def safe_return_page(page: Page, page_pool: asyncio.Queue) -> None:
    try:
        await reset_page(page)
    except Exception as e:
        logging.warning(f"Error resetting page: {e}")
    finally:
        await page_pool.put(page)

async def downloader(
    context: BrowserContext,
    url: str,
    output_folder: str,
    semaphore: asyncio.Semaphore,
    args: argparse.Namespace,
    page_pool: asyncio.Queue,
    max_retries: int = 3,
) -> None:
    global shutdown_flag
    async with semaphore:
        if shutdown_flag:
            logging.info(f"Shutdown initiated, skipping URL: {url}")
            return

        url = clean_url(url)
        async with downloaded_urls_lock:
            if url in downloaded_urls:
                logging.info(f"URL {url} already processed. Skipping.")
                return

        encoded_url = encode_url(url)
        parsed_url = urlparse(url)
        sanitized_path = sanitize_filename(parsed_url.path.strip('/')) or "index"

        try:
            head_response = await context.request.fetch(encoded_url, method="HEAD")
            head_content_type = head_response.headers.get("content-type", "").lower()
        except Exception:
            head_content_type = ""

        _, ext = os.path.splitext(parsed_url.path)
        ext = ext.lower()
        is_html = (not ext or (head_content_type and "text/html" in head_content_type) or ext in {".html", ".htm"})
        download_method = "html" if is_html else "static"

        response_obj: Optional[Response] = None
        content = None
        write_mode = None
        encoding = None
        file_extension = None

        if download_method == "html":
            file_extension = "html"
            write_mode = "w"
            encoding = "utf-8"
            page: Page = await page_pool.get()
            try:
                for attempt in range(max_retries):
                    try:
                        response_obj = await page.goto(encoded_url, wait_until="networkidle", timeout=30000)
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        break
                    except Exception as e:
                        if attempt < max_retries - 1:
                            delay = 2 ** (attempt + 1)
                            logging.warning(f"Attempt {attempt+1} failed for {url}: {e}. Retrying in {delay} seconds...")
                            await reset_page(page)
                            await asyncio.sleep(delay)
                        else:
                            logging.error(f"Failed to download {url} after {max_retries} attempts: {e}")
                            return
                if not response_obj or response_obj.status != 200:
                    logging.error(f"Response error for {url}, status: {response_obj.status if response_obj else 'None'}. Skipping.")
                    return
                try:
                    content = await page.content()
                except Exception as e:
                    logging.error(f"Error retrieving content for {url}: {e}")
                    return
            finally:
                await safe_return_page(page, page_pool)
        else:
            try:
                response_obj = await context.request.get(encoded_url)
            except Exception as e:
                logging.error(f"Error fetching {url} via request: {e}")
                return

            if response_obj.status != 200:
                logging.error(f"Response status for {url} is {response_obj.status}, skipping download.")
                return

            if ext:
                file_extension = ext[1:]
            else:
                file_extension = get_extension_from_header(response_obj.headers.get("content-type", ""))

            text_extensions = {"js", "css", "json", "html", "htm", "txt"}
            if file_extension in text_extensions:
                try:
                    content = await response_obj.text()
                except Exception as e:
                    logging.error(f"Error reading text content from {url}: {e}")
                    return
                if file_extension == "js":
                    content = beautify(content)
                write_mode = "w"
                encoding = "utf-8"
            else:
                try:
                    content = await response_obj.body()
                except Exception as e:
                    logging.error(f"Error reading binary content from {url}: {e}")
                    return
                write_mode = "wb"
                encoding = None

        # For HTML files, normalize the content before hashing to ignore dynamic differences.
        if file_extension == "html":
            normalized_content = normalize_html(content)
            content_bytes = normalized_content.encode('utf-8')
        else:
            content_bytes = content.encode('utf-8') if write_mode == "w" else content

        content_hash = hashlib.md5(content_bytes).hexdigest()

        # Check if duplicate content exists regardless of URL.
        if content_hash in downloaded_hashes:
            filename = downloaded_hashes[content_hash]
            logging.info(f"Duplicate content detected for {url}. Already downloaded as {filename}.")
            await append_url_mapping(filename, url)
            await append_downloaded_url(url)
            return

        # New filename: include sanitized path and 8 characters of content hash.
        filename = f"{sanitized_path}_{content_hash[:8]}.{file_extension}"
        subfolder = os.path.join(output_folder, file_extension)
        if not await asyncio.to_thread(os.path.exists, subfolder):
            await asyncio.to_thread(os.makedirs, subfolder, exist_ok=True)
        file_path = os.path.join(subfolder, filename)

        try:
            if await asyncio.to_thread(os.path.exists, file_path) and not args.overwrite:
                logging.info(f"Content for {url} already exists as {filename}. Skipping save.")
            else:
                temp_file_path = file_path + ".tmp"
                if write_mode == "w":
                    async with aiofiles.open(temp_file_path, write_mode, encoding=encoding) as f:
                        await f.write(content)
                else:
                    async with aiofiles.open(temp_file_path, write_mode) as f:
                        await f.write(content)
                await asyncio.to_thread(os.replace, temp_file_path, file_path)
                logging.info(f"Downloaded: {filename}")

            await append_url_mapping(filename, url)
            await append_downloaded_url(url)
            await append_downloaded_hash(content_hash, filename)
        except Exception as e:
            logging.error(f"Error processing {url}: {e}")
            if await asyncio.to_thread(os.path.exists, temp_file_path):
                try:
                    await asyncio.to_thread(os.remove, temp_file_path)
                except Exception as rm_e:
                    logging.error(f"Error removing temporary file {temp_file_path}: {rm_e}")
            return

async def main() -> None:
    parser = argparse.ArgumentParser(description="Combined Downloader with enhanced URL handling and duplicate detection using normalized HTML.")
    parser.add_argument("input_file", help="File containing list of URLs (one per line)")
    parser.add_argument("output_folder", help="Output folder for downloaded files")
    parser.add_argument("--concurrency", type=int, default=5, help="Maximum concurrent downloads (default: 5)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--logfile", help="Optional log file to write output to")
    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    handler = TqdmLoggingHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if args.logfile:
        file_handler = logging.FileHandler(args.logfile)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.get_running_loop()
    def shutdown_handler() -> None:
        global shutdown_flag
        logging.info("Shutdown signal received, no new tasks will be started.")
        shutdown_flag = True

    if os.name == "nt":
        signal.signal(signal.SIGINT, lambda s, f: shutdown_handler())
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler())
    else:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)

    global downloaded_urls, downloaded_hashes
    downloaded_urls = await load_downloaded_urls()
    downloaded_hashes = await load_downloaded_hashes()

    if not os.path.exists(downloaded_urls_file):
        open(downloaded_urls_file, "a").close()
    if not os.path.exists(mapping_file):
        with open(mapping_file, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["filename", "url"])

    if not os.path.exists(args.output_folder):
        await asyncio.to_thread(os.makedirs, args.output_folder, exist_ok=True)

    async with aiofiles.open(args.input_file, "r") as f:
        lines = await f.readlines()
    all_urls = {line.strip() for line in lines if line.strip()}
    urls = [url for url in all_urls if url not in downloaded_urls]
    if not urls:
        logging.info("No new URLs to download. Exiting.")
        return

    semaphore = asyncio.Semaphore(args.concurrency)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context: BrowserContext = await browser.new_context(user_agent="Mozilla/5.0 (compatible)")

        page_pool: asyncio.Queue = asyncio.Queue()
        for _ in range(args.concurrency):
            page = await context.new_page()
            await page_pool.put(page)

        tasks = [
            downloader(context, url, args.output_folder, semaphore, args, page_pool)
            for url in urls
        ]
        pbar = tqdm(total=len(urls), desc="Downloading URLs")
        for task in asyncio.as_completed(tasks):
            await task
            pbar.update(1)
        pbar.close()

        while not page_pool.empty():
            page: Page = await page_pool.get()
            await page.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
