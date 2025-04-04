## downloader.py

Python Script to Download a list of URLs efficiently. Especially the html files if you need the browser to load the DOM before downloading. I first wrote it on my own back in 2023 to only download beautified Js files.

## Usage

- Basic Usage:
  
```sh
downloader.py input_file output_folder
```

- All available args:

```py
usage: downloader.py [-h] [--concurrency CONCURRENCY] [--overwrite] [--logfile LOGFILE] [--markdown]
                     input_file output_folder

Combined Downloader with enhanced URL handling and optional HTML to Markdown conversion.

positional arguments:
  input_file            File containing list of URLs (one per line)
  output_folder         Output folder for downloaded files

options:
  -h, --help            show this help message and exit
  --concurrency CONCURRENCY
                        Maximum concurrent downloads (default: 5)
  --overwrite           Overwrite existing files
  --logfile LOGFILE     Optional log file to write output to
  --markdown            Convert HTML files to Markdown (.md) instead of saving as HTML

```

