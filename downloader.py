import os
import re
import asyncio
import logging
import argparse
import signal
import hashlib
from typing import Set, Optional
from urllib.parse import urlparse, unquote

import aiofiles
from jsbeautifier import beautify
from playwright.async_api import async_playwright, BrowserContext, Page, Response

# Global shutdown flag for graceful termination.
shutdown_flag: bool = False

# File to store processed URLs.
downloaded_urls_file: str = "downloaded_urls.txt"
downloaded_urls: Set[str] = set()
downloaded_urls_lock = asyncio.Lock()

# Mapping file to map short filenames to original URLs.
mapping_file: str = "url_mapping.txt"
mapping_lock = asyncio.Lock()

# Globals for caching existing output files to speed up skip checks.
existing_files: Set[str] = set()
existing_files_lock = asyncio.Lock()

MAX_FILENAME_LENGTH = 255  # Maximum allowed filename length.

async def load_downloaded_urls() -> Set[str]:
    """Asynchronously load the list of already downloaded URLs from a text file."""
    if await asyncio.to_thread(os.path.exists, downloaded_urls_file):
        async with aiofiles.open(downloaded_urls_file, "r") as f:
            lines = await f.readlines()
            return {line.strip() for line in lines if line.strip()}
    return set()

async def append_downloaded_url(url: str) -> None:
    """Append a newly downloaded URL to the file asynchronously and update the in-memory set."""
    async with downloaded_urls_lock:
        async with aiofiles.open(downloaded_urls_file, "a") as f:
            await f.write(url + "\n")
        downloaded_urls.add(url)

async def append_url_mapping(short_filename: str, url: str) -> None:
    """Append a mapping from the short filename to the original URL."""
    async with mapping_lock:
        async with aiofiles.open(mapping_file, "a") as f:
            await f.write(f"{short_filename}\t{url}\n")

def sanitize_filename(filename: str) -> str:
    """Sanitizes filename by removing or replacing special characters."""
    filename = unquote(filename)
    return re.sub(r'[\\/:*?"<>|]', '_', filename)

def get_extension(content_type: str) -> str:
    """Determines the appropriate file extension based on Content-Type."""
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

def get_filename_from_content_disposition(header_value: Optional[str]) -> Optional[str]:
    """
    Extracts a filename from the Content-Disposition header if available.
    Expected format: attachment; filename="example.txt"
    """
    if not header_value:
        return None
    parts = header_value.split(';')
    for part in parts:
        if 'filename=' in part:
            filename = part.split('=', 1)[1].strip().strip('"')
            return filename
    return None

async def reset_page(page: Page) -> None:
    """
    Attempts to reset the page state by navigating to 'about:blank'.
    This helps clear any residual state from previous navigation.
    """
    try:
        await page.goto("about:blank", wait_until='domcontentloaded', timeout=10000)
    except Exception as e:
        logging.warning(f"Failed to reset page state: {e}")

async def safe_return_page(page: Page, page_pool: asyncio.Queue) -> None:
    """
    Resets the page and then returns it to the pool.
    """
    try:
        await reset_page(page)
    except Exception as e:
        logging.warning(f"Failed to reset page before returning to pool: {e}")
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
    """
    Downloads content from the URL using a shared browser context.
    Implements retry with exponential backoff, uses a reusable page from the pool,
    performs integrity checks, writes to a temporary file, and records the URL.
    """
    global shutdown_flag
    async with semaphore:
        if shutdown_flag:
            logging.info(f"Shutdown initiated, skipping URL: {url}")
            return

        # Check if URL has already been processed.
        async with downloaded_urls_lock:
            if url in downloaded_urls:
                logging.info(f"URL {url} already processed. Skipping.")
                return

        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        sanitized_path = sanitize_filename(parsed_url.path.strip('/'))
        if not sanitized_path:
            sanitized_path = 'index'

        # Ensure the output folder exists (using exist_ok=True via threaded call).
        if not await asyncio.to_thread(os.path.exists, output_folder):
            await asyncio.to_thread(os.makedirs, output_folder, exist_ok=True)

        # Acquire a page from the pool.
        page: Page = await page_pool.get()
        response_obj: Optional[Response] = None

        try:
            # Exponential backoff retry mechanism.
            for attempt in range(max_retries):
                try:
                    response_obj = await page.goto(url, wait_until='networkidle', timeout=30000)
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        delay = 2 ** (attempt + 1)  # 2, 4, 8 seconds, etc.
                        logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}. Retrying in {delay} seconds...")
                        await reset_page(page)
                        await asyncio.sleep(delay)
                    else:
                        logging.error(f"Failed to download {url} after {max_retries} attempts: {e}")
                        return
            if not response_obj:
                logging.error(f"No response received for {url}")
                return

            status = response_obj.status
            if status < 200 or status >= 300:
                logging.error(f"URL {url} returned status code {status}. Skipping.")
                return

            content_type = response_obj.headers.get('content-type', '')
            content_disposition = response_obj.headers.get('content-disposition', None)
            # Note: filename_from_header is extracted but not used, because we're generating our own short filename.
            filename_from_header = get_filename_from_content_disposition(content_disposition)

            # Determine file extension.
            file_extension = get_extension(content_type)

            # Generate a short filename using a snippet of the URL path and an MD5 hash.
            context_part = sanitized_path[:50] if sanitized_path else 'index'
            hash_val = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
            short_filename = f"{context_part}_{hash_val}.{file_extension}"
            if len(short_filename) > MAX_FILENAME_LENGTH:
                base, ext = os.path.splitext(short_filename)
                allowed_length = MAX_FILENAME_LENGTH - len(ext)
                base = base[:allowed_length]
                short_filename = base + ext

            file_path = os.path.join(output_folder, short_filename)
            
            # Check if the file already exists (using cached filenames) and skip if not overwriting.
            async with existing_files_lock:
                if short_filename in existing_files and not args.overwrite:
                    logging.info(f"File {file_path} already exists. Skipping.")
                    await append_downloaded_url(url)
                    return

            # Decide between text and binary writing based on content-type.
            if any(sub in content_type for sub in ['text', 'json', 'javascript', 'css']) or file_path.endswith(('.html', '.txt', '.json', '.js', '.css')):
                if file_path.endswith('.html'):
                    content = await page.content()
                else:
                    content = await response_obj.text()
                if file_path.endswith('.js'):
                    content = beautify(content)
                write_mode = 'w'
                encoding = 'utf-8'
            else:
                content = await response_obj.body()  # binary content
                write_mode = 'wb'
                encoding = None

            # Write to a temporary file first.
            temp_file_path = file_path + ".tmp"
            try:
                if write_mode == 'w':
                    async with aiofiles.open(temp_file_path, write_mode, encoding=encoding) as f:
                        await f.write(content)
                else:
                    async with aiofiles.open(temp_file_path, write_mode) as f:
                        await f.write(content)
                # Rename the temporary file to its final name.
                await asyncio.to_thread(os.replace, temp_file_path, file_path)

                # Integrity check: if content-length is provided, compare file size.
                expected_length_str = response_obj.headers.get("content-length")
                if expected_length_str:
                    try:
                        expected_length = int(expected_length_str)
                        actual_length = await asyncio.to_thread(os.path.getsize, file_path)
                        if actual_length != expected_length:
                            logging.error(f"Integrity check failed for {short_filename}: expected {expected_length}, got {actual_length}")
                            await asyncio.to_thread(os.remove, file_path)
                            return
                    except Exception as e:
                        logging.warning(f"Integrity check error for {short_filename}: {e}")

                logging.info(f"Downloaded: {short_filename}")
                await append_downloaded_url(url)
                await append_url_mapping(short_filename, url)
                
                # Update the cache with the new file.
                async with existing_files_lock:
                    existing_files.add(short_filename)
            except Exception as e:
                logging.error(f"Error saving file for {url}: {e}")
                if await asyncio.to_thread(os.path.exists, temp_file_path):
                    await asyncio.to_thread(os.remove, temp_file_path)
                return
        finally:
            # Ensure the page is always returned to the pool.
            if page is not None:
                await safe_return_page(page, page_pool)

async def main() -> None:
    parser = argparse.ArgumentParser(description="Download URLs using Playwright.")
    parser.add_argument("input_file", help="File containing list of URLs (one per line)")
    parser.add_argument("output_folder", help="Output folder for downloaded files")
    parser.add_argument("--concurrency", type=int, default=5, help="Maximum number of concurrent downloads (default: 5)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--logfile", help="Optional log file to write output to")
    args = parser.parse_args()

    # Logging setup: add file handler if a logfile is provided.
    if args.logfile:
        file_handler = logging.FileHandler(args.logfile)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # For Windows compatibility.
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Register signal handlers for graceful shutdown.
    loop = asyncio.get_running_loop()
    def shutdown_handler() -> None:
        global shutdown_flag
        logging.info("Shutdown signal received, no new tasks will be started.")
        shutdown_flag = True

    if os.name == 'nt':
        signal.signal(signal.SIGINT, lambda s, f: shutdown_handler())
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler())
    else:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)

    global downloaded_urls, existing_files, existing_files_lock
    downloaded_urls = await load_downloaded_urls()

    # Ensure the downloaded URLs and mapping file exist.
    if not os.path.exists(downloaded_urls_file):
        open(downloaded_urls_file, 'a').close()
    if not os.path.exists(mapping_file):
        open(mapping_file, 'a').close()

    # Ensure the output folder exists (using threaded call with exist_ok=True).
    if not os.path.exists(args.output_folder):
        await asyncio.to_thread(os.makedirs, args.output_folder, exist_ok=True)
    existing_files = set(os.listdir(args.output_folder))
    existing_files_lock = asyncio.Lock()

    # Read and deduplicate URLs from the input file.
    async with aiofiles.open(args.input_file, "r") as f:
        lines = await f.readlines()
    all_urls = {line.strip() for line in lines if line.strip()}
    # Pre-filter URLs by removing those already processed.
    urls = [url for url in all_urls if url not in downloaded_urls]
    if not urls:
        logging.info("No new URLs to download. Exiting.")
        return

    semaphore = asyncio.Semaphore(args.concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context: BrowserContext = await browser.new_context(user_agent="Mozilla/5.0 (compatible; MyDownloader/1.0)")

        # Create a pool of pages for reuse.
        page_pool: asyncio.Queue = asyncio.Queue()
        for _ in range(args.concurrency):
            page = await context.new_page()
            await page_pool.put(page)

        tasks = [
            downloader(context, url, args.output_folder, semaphore, args, page_pool)
            for url in urls
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logging.info("Tasks were cancelled during shutdown.")
        finally:
            # Close all pages.
            while not page_pool.empty():
                page: Page = await page_pool.get()
                await page.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
