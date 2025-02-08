# Job Scraping and Qualification Extraction

This project scrapes job listings from multiple job boards and online sources, extracts relevant information (such as job titles, company names, locations, salary, type of job, and qualifications), and saves the data to a CSV file. The goal is to gather and structure job data for data science roles to analyze requirements and identify trends in qualifications and skills.

## Features

- Scrapes job data from various sources:
  - **TheLadders**
  - **CareerBuilder**
  - **Google Jobs** (via the Google Search API)

- Extracts job details including:
  - Job title
  - Company name
  - Job location (city, state)
  - Salary (if available)
  - Job type (full-time, part-time, etc.)
  - Minimum qualifications required
  - Desired qualifications
  - Relevant skills (e.g., Python, SQL, Cloud)

- Stores the scraped data in a CSV file (`group_7_dsc_jobs.csv`) for further analysis.

## Requirements

To run this project, you'll need the following Python libraries:

- `beautifulsoup4` (for web scraping)
- `http3` (for making HTTP requests)
- `googlesearch-python` (for Google Jobs API)
- `csv` (for writing data to CSV)
- `requests` (for general HTTP requests)

You can install the required libraries using `pip`:

```bash
pip install beautifulsoup4 http3 googlesearch-python requests
```

## Usage

### Step 1: Set up the API key for Google Jobs

Before running the script, ensure you have an API key for the Google Jobs API. Replace the placeholder in the code with your actual API key:

```python
"api_key": "YOUR_GOOGLE_API_KEY"
```

### Step 2: Run the Script

To run the script, execute the following in your terminal:

```python
python job_scraper.py
```

The script will prompt you with a menu to choose which part to run:

- Covid Data (currently not implemented, but you can extend the script to handle this).
- Jobs Data: This part scrapes job listings from TheLadders, CareerBuilder, and Google Jobs.

## Potential Issues and Workarounds
API time limits: If you encounter time limits or rate limits when using the Google Jobs API, it may be due to the restrictions set by the API on the amount of data that can be fetched within a specific time period. If this occurs, you might want to consider using a VPN to change your IP address and avoid getting blocked or throttled by the API provider.
