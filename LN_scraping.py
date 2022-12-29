# Import libraries
import logging
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, RemoteFilters
import pandas as pd
import time


# Set running time counter
start_time = time.time()


# Change root logger level (default is WARN)
# logging.basicConfig(level=logging.INFO)

# create empty LinkedIn Job posting lists
job_title = []
job_company = []
job_company_link = []
job_date = []
job_link = []
job_insights = []
job_description = []


# Fired once for each successfully processed job
def on_data(data: EventData):
    print('[ON_DATA]', data.title, ',', data.company, ',', data.company_link, ',', data.date, ',', data.link, ',',
          data.insights, ',', len(data.description))
    job_title.append(data.title)
    job_company.append(data.company)
    job_company_link.append(data.company_link)
    job_date.append(data.date)
    job_link.append(data.link)
    job_insights.append(data.insights)
    job_description.append(data.description)


# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))


def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


scraper = LinkedinScraper(
    chrome_executable_path=None,  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver)
    chrome_options=None,  # Custom Chrome options here
    headless=True,  # Overrides headless mode only if chrome_options is None
    max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    slow_mo=3.5,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
    page_load_timeout=20  # Page load timeout (in seconds)
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)


# Define a list of jobs to query
q = ['data scientist', 'data engineer', 'machine learning', 'data mining', 'data analyst', 'business analyst', 'python', 'sql', 'database']

# Initialize i for the query loop
i = 0


# Query loop
for i in range(len(q)):
    # while (i < len(q)):
    queries = [
        Query(
            query=str(q[i]),
            options=QueryOptions(
                locations=['Italy'],
                apply_link=True,  # Try to extract apply link (easy applies are skipped). Default to False.
                limit=20,  # how many job scraped for each q[i]
                filters=QueryFilters(
                    # company_jobs_url='https://www.linkedin.com/jobs/search/?f_C=1441%2C17876832%2C791962%2C2374003%2C18950635%2C16140%2C10440912&geoId=92000000',  # Filter by companies.
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                    experience= ExperienceLevelFilters.ENTRY_LEVEL,
                    # remote = RemoteFilters.REMOTE   #set None if not work from home job
                )
            )
        )
    ]
    scraper.run(queries)
    # i = i+1   #non serve, è uguale


# Print data
data = {'title': job_title, 'company': job_company, 'company link': job_company_link, 'date': job_date,
        'link': job_link, 'insights': job_insights, 'description': job_description}
df = pd.DataFrame(data)
print('[RESULT1]', df.to_string(index=False))
df = df.loc[df.astype(str).drop_duplicates(subset=['title', 'company', 'date', 'description']).index].reset_index(drop = True)  #drop row if these 4 columns are the same
print('[RESULT2]', df)


# Save to a csv file
df.to_csv("job_postings.csv", index = False)


# Running time
print("--- Running time: %s seconds ---" % (time.time() - start_time))
