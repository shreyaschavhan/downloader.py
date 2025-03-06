## downloader.py

Python Script to Download a list of URLs efficiently. Especially the html files if you need the browser to load the DOM before downloading.

## Usage

- Basic Usage:
  
```sh
downloader.py input_file output_file
```

- All available args:

```py
 parser = argparse.ArgumentParser(description="Download URLs using Playwright.")
    parser.add_argument("input_file", help="File containing list of URLs (one per line)")
    parser.add_argument("output_folder", help="Output folder for downloaded files")
    parser.add_argument("--concurrency", type=int, default=5, help="Maximum number of concurrent downloads (default: 5)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--logfile", help="Optional log file to write output to")
```

