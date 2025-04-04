import os
import re
import asyncio
import logging
import argparse
import signal
import hashlib
from typing import Set, Optional
from urllib.parse import urlparse, unquote, urlsplit, urlunsplit, quote

import aiofiles
from jsbeautifier import beautify
from playwright.async_api import async_playwright, BrowserContext, Page, Response
from tqdm import tqdm

# Custom logging handler that uses tqdm.write() to output messages.
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

# Helper function: percent-encode URL's path and query.
def encode_url(url: str) -> str:
    parts = urlsplit(url)
    encoded_path = quote(parts.path, safe="/")
    encoded_query = quote(parts.query, safe="=&")
    return urlunsplit((parts.scheme, parts.netloc, encoded_path, encoded_query, parts.fragment))

# Helper function: remove any null bytes from URL.
def clean_url(url: str) -> str:
    return url.replace("\x00", "")

# Global shutdown flag for graceful termination.
shutdown_flag: bool = False

# File to store processed URLs.
downloaded_urls_file: str = "downloaded_urls.txt"
downloaded_urls: Set[str] = set()
downloaded_urls_lock = asyncio.Lock()

# Mapping file to map short filenames to original URLs.
mapping_file: str = "url_mapping.txt"
mapping_lock = asyncio.Lock()

MAX_FILENAME_LENGTH = 255  # Maximum allowed filename length.

async def load_downloaded_urls() -> Set[str]:
    """Load list of already downloaded URLs asynchronously."""
    if await asyncio.to_thread(os.path.exists, downloaded_urls_file):
        async with aiofiles.open(downloaded_urls_file, "r") as f:
            lines = await f.readlines()
            return {line.strip() for line in lines if line.strip()}
    return set()

async def append_downloaded_url(url: str) -> None:
    """Append a URL to the downloaded list and update in‑memory set."""
    async with downloaded_urls_lock:
        async with aiofiles.open(downloaded_urls_file, "a") as f:
            await f.write(url + "\n")
        downloaded_urls.add(url)

async def append_url_mapping(short_filename: str, url: str) -> None:
    """Append a mapping from short filename to the original URL."""
    async with mapping_lock:
        async with aiofiles.open(mapping_file, "a") as f:
            await f.write(f"{short_filename}\t{url}\n")

def sanitize_filename(filename: str) -> str:
    """Sanitize filenames by replacing special characters."""
    filename = unquote(filename)
    return re.sub(r'[\\/:*?"<>|]', '_', filename)

def get_extension_from_header(content_type: str) -> str:
    """Determine file extension from the Content-Type header if needed."""
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
    """Reset the page by navigating to 'about:blank'."""
    try:
        await page.goto("about:blank", wait_until="domcontentloaded", timeout=10000)
    except Exception as e:
        logging.warning(f"Failed to reset page state: {e}")

async def safe_return_page(page: Page, page_pool: asyncio.Queue) -> None:
    """Reset and return the page to the pool."""
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
    Downloads a URL.

    If a HEAD request shows the resource's Content-Type is HTML (or if the URL extension is .html/.htm
    or is missing, we assume HTML), full browser navigation (page.goto) is used so that dynamic content loads.
    In that case, the file is always saved with a .html extension.
    For all other resources, a direct request (context.request.get) is used.
    
    Files are saved into subfolders (by file type) with a short generated filename.
    """
    global shutdown_flag
    async with semaphore:
        if shutdown_flag:
            logging.info(f"Shutdown initiated, skipping URL: {url}")
            return

        # Clean URL from null bytes.
        url = clean_url(url)
        
        # Skip already processed URLs.
        async with downloaded_urls_lock:
            if url in downloaded_urls:
                logging.info(f"URL {url} already processed. Skipping.")
                return

        # Always use encoded URL for requests.
        encoded_url = encode_url(url)
        parsed_url = urlparse(url)
        sanitized_path = sanitize_filename(parsed_url.path.strip('/'))
        if not sanitized_path:
            sanitized_path = "index"

        # Determine download method using a HEAD request.
        try:
            head_response = await context.request.fetch(encoded_url, method="HEAD")
            head_content_type = head_response.headers.get("content-type", "").lower()
        except Exception as e:
            logging.warning(f"HEAD request failed for {url}: {e}")
            head_content_type = ""

        _, ext = os.path.splitext(parsed_url.path)
        ext = ext.lower()
        # If no extension, assume HTML.
        if not ext:
            is_html = True
        else:
            is_html = False
        html_extensions = {".html", ".htm"}
        if head_content_type and "text/html" in head_content_type:
            is_html = True
        elif not head_content_type and ext in html_extensions:
            is_html = True

        download_method = "html" if is_html else "static"

        response_obj: Optional[Response] = None
        content = None

        if download_method == "html":
            # For HTML pages, force file extension to be "html".
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
                if not response_obj:
                    logging.error(f"No response received for {url}")
                    return
                if response_obj.status != 200:
                    logging.error(f"Response status for {url} is {response_obj.status}, skipping download.")
                    return
                try:
                    content = await page.content()
                except Exception as e:
                    logging.error(f"Error retrieving page content for {url}: {e}")
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

        # Create a subfolder for the file type.
        subfolder = os.path.join(output_folder, file_extension)
        if not await asyncio.to_thread(os.path.exists, subfolder):
            await asyncio.to_thread(os.makedirs, subfolder, exist_ok=True)

        context_part = sanitized_path[:50] if sanitized_path else "index"
        hash_val = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
        short_filename = f"{context_part}_{hash_val}.{file_extension}"
        if len(short_filename) > MAX_FILENAME_LENGTH:
            base, ext_part = os.path.splitext(short_filename)
            allowed_length = MAX_FILENAME_LENGTH - len(ext_part)
            base = base[:allowed_length]
            short_filename = base + ext_part

        file_path = os.path.join(subfolder, short_filename)

        if await asyncio.to_thread(os.path.exists, file_path) and not args.overwrite:
            logging.info(f"File {file_path} already exists. Skipping.")
            await append_downloaded_url(url)
            return

        temp_file_path = file_path + ".tmp"
        try:
            if write_mode == "w":
                async with aiofiles.open(temp_file_path, write_mode, encoding=encoding) as f:
                    await f.write(content)
            else:
                async with aiofiles.open(temp_file_path, write_mode) as f:
                    await f.write(content)
            await asyncio.to_thread(os.replace, temp_file_path, file_path)

            logging.info(f"Downloaded: {short_filename}")
            await append_downloaded_url(url)
            await append_url_mapping(short_filename, url)
        except Exception as e:
            logging.error(f"Error saving file for {url}: {e}")
            if await asyncio.to_thread(os.path.exists, temp_file_path):
                try:
                    await asyncio.to_thread(os.remove, temp_file_path)
                except Exception as e:
                    logging.error(f"Error removing temporary file {temp_file_path}: {e}")
            return

async def main() -> None:
    parser = argparse.ArgumentParser(description="Download URLs using Playwright.")
    parser.add_argument("input_file", help="File containing list of URLs (one per line)")
    parser.add_argument("output_folder", help="Output folder for downloaded files")
    parser.add_argument("--concurrency", type=int, default=5, help="Maximum concurrent downloads (default: 5)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--logfile", help="Optional log file to write output to")
    args = parser.parse_args()

    # Set up custom logging to use tqdm.write()
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

    global downloaded_urls
    downloaded_urls = await load_downloaded_urls()

    if not os.path.exists(downloaded_urls_file):
        open(downloaded_urls_file, "a").close()
    if not os.path.exists(mapping_file):
        open(mapping_file, "a").close()

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

        # Create a pool of pages for HTML (dynamic) requests.
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
