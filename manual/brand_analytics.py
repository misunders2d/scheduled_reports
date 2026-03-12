import asyncio

import connection
import pandas as pd
import pandas_gbq

from reports.process_reports import check_and_download_report
from reports.report_types import brand_analytics_report

documents = []

response = asyncio.run(brand_analytics_report())
report_document = asyncio.run(check_and_download_report(response))
if not report_document:
    raise BaseException("Report could not be downloaded.")
documents.append(report_document)

full_df = pd.DataFrame()
all_rows = []

for i, doc in enumerate(documents, start=1):
    print(f"Processing document {i} of {len(documents)}")
    asin_data = doc["dataByAsin"]
    for asin_row in asin_data:
        temp_df = pd.json_normalize(asin_row)
        temp_df = temp_df.dropna(axis=1, how="all")
        # full_df = pd.concat([full_df, temp_df])
        all_rows.append(temp_df)

# print(full_df.columns)
full_df = pd.concat(all_rows)
full_df.columns = [
    x.replace(".", "_").lower().strip() for x in full_df.columns.tolist()
]
print(full_df.shape)
confirmation = input("Upload to GBQ? (y/n): ")
if confirmation.lower() != "y":
    print("Upload cancelled.")
    print(
        f"Document id for reference: {response.payload["reportId"] if response is not None else "N/A"}"
    )
    exit()
pandas_gbq.to_gbq(
    full_df,
    destination_table="mellanni-project-da.auxillary_development.scp_asin_weekly",
    if_exists="append",
    credentials=connection.create_credentials(),
)

print("All done")

combine_query = """
SELECT s.startdate, s.asin, s.impressiondata_impressioncount, d.collection
FROM `mellanni-project-da.auxillary_development.scp_asin_weekly` as s
LEFT JOIN (SELECT DISTINCT(asin), collection FROM `mellanni-project-da.auxillary_development.dictionary`) as d
on s.asin = d.asin

ORDER BY startdate, impressiondata_impressioncount DESC

"""
