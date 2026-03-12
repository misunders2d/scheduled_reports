import asyncio
import logging
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
from reports.process_reports import check_and_download_report, fetch_reports
from reports.report_types import ReportType, brand_analytics_report
from sp_utils import chunk_asins, convert_date_to_isoformat, send_telegram_message

from connection import bigquery, connect_to_bigquery, create_credentials

logging.basicConfig(
    filename="sqp_log.log",
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


async def check_if_ba_report_exists(document):
    asins = document["reportSpecification"].get("reportOptions", {}).get("asin")
    print(f"Checking asins: {asins}")
    asins = [x.strip() for x in asins.split()]
    start_date = datetime.strptime(
        document["reportSpecification"].get("dataStartTime"), "%Y-%m-%d"
    ).date()
    period = (
        document["reportSpecification"].get("reportOptions", {}).get("reportPeriod")
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("asins", "STRING", asins),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("period", "STRING", period),
        ]
    )
    query = """
    SELECT DISTINCT asin
    FROM `mellanni-project-da.auxillary_development.sqp_asin_weekly`
    WHERE DATE(startDate) = @start_date
      AND period = @period
      AND asin IN UNNEST(@asins)
      """

    with connect_to_bigquery() as client:
        bq_result = client.query(query, job_config=job_config)
    duplicate_asins = {x.asin for x in bq_result}
    unique_asins = [x for x in asins if x not in duplicate_asins]
    if duplicate_asins:
        print(
            f"[[DUPLICATES]] {len(duplicate_asins)} duplicate asins found for {start_date} {period}: ",
            ", ".join(duplicate_asins),
        )
    if unique_asins:
        print(
            f"[[UNIQUE]] {len(unique_asins)} unique asins found for {start_date} {period}: ",
            ", ".join(unique_asins),
        )
    return unique_asins


def process_document(document):
    result = pd.DataFrame()
    columns = dict()

    def process_row(row, prefix=None):
        for key, value in row.items():
            if isinstance(value, dict):
                process_row(value, prefix=key)
            else:
                key = f"{prefix}_{key}" if prefix else key
                columns[key] = value
        return columns

    for row in document["dataByAsin"]:
        columns = process_row(row)
        result = pd.concat(
            [
                result,
                pd.DataFrame(data=[columns.values()], columns=pd.Index(columns.keys())),
            ]
        )
    period = (
        document["reportSpecification"].get("reportOptions", {}).get("reportPeriod")
    )
    asins = document["reportSpecification"].get("reportOptions", {}).get("asin")
    asins = [x.strip() for x in asins.split()]

    start_date = datetime.strptime(
        document["reportSpecification"].get("dataStartTime"), "%Y-%m-%d"
    ).date()
    marketplaces = document["reportSpecification"].get("marketplaceIds", [])

    if len(document["dataByAsin"]) == 0:

        result["asin"] = asins
        result["startDate"] = start_date

    result["period"] = period
    result["marketplaces"] = ", ".join(sorted(marketplaces))
    return result


async def upload_ba_report(document):
    document_specs = document.get("reportSpecification", "")
    try:
        unique_asins_job = check_if_ba_report_exists(document)
        report_df = process_document(document)

        unique_asins = await unique_asins_job

        report_to_upload = report_df.loc[report_df["asin"].isin(unique_asins)]
        if len(report_to_upload) == 0:
            print("[[RESULT]] All records are duplicates, skipping")
        else:
            print(f"[[RESULT]] Uploading {len(report_to_upload)} rows to bigquery")
            pandas_gbq.to_gbq(
                report_to_upload,
                destination_table="mellanni-project-da.auxillary_development.sqp_asin_weekly",
                credentials=create_credentials(),
                if_exists="append",
            )
        return {"status": "success", "document": document_specs}
    except Exception as e:
        return {"status": "failed", "error": e, "document": document_specs}


async def collect_sqp_reports(created_since, created_before):
    print(f"[[DATE: {created_since} to {created_before}]]")
    created_since = (
        convert_date_to_isoformat(created_since)
        if isinstance(created_since, datetime)
        else created_since
    )
    created_before = (
        convert_date_to_isoformat(created_before)
        if isinstance(created_before, datetime)
        else created_before
    )

    try:
        all_reports = await fetch_reports(
            report_types=[
                ReportType.GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT
            ],
            processing_statuses=["DONE"],
            created_since=created_since,
            created_before=created_before,
        )
        for i, report_record in enumerate(all_reports, start=1):
            document = await check_and_download_report(
                report_id=report_record["reportId"]
            )
            result = await upload_ba_report(document=document)
            if result["status"] == "failed":
                await send_telegram_message(
                    message=f"Failed to process BA report. Error: {result['error']}, document: {result['document']}"
                )
            print(f"Uploaded {i} reports of {len(all_reports)}", end="\n\n")
    except Exception as e:
        print(f"[[ERROR for {str(e)}]]: {e}\nRetrying...")
        await collect_sqp_reports(
            created_since=created_since,
            created_before=created_before,
        )


async def run_sqp_reports(
    start_dates: list[str | datetime] | str, asins: list[str] | str
):
    """
    Downloads SQP reports for a given selection of dates and for a given set of ASINs.
    ASINs are chunked 18 at a time.
    """
    asins_list = chunk_asins(asins)
    start_dates_clean = (
        [convert_date_to_isoformat(d) for d in start_dates]
        if isinstance(start_dates, list)
        else [convert_date_to_isoformat(start_dates)]
    )
    ba_report_jobs = []
    for week_start in start_dates_clean:
        for asin_chunk in asins_list:
            ba_report_jobs.append(
                brand_analytics_report(
                    week_start=week_start,
                    report_type=ReportType.GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT,
                    asin=asin_chunk,
                )
            )

    responses = []
    for ba_report_job in ba_report_jobs:
        responses.append(await ba_report_job)

    document_jobs = []
    for response in responses:
        document_jobs.append(check_and_download_report(response=response))

    report_documents = []
    for document_job in document_jobs:
        report_documents.append(await document_job)

    ba_uploads = []
    for report_document in report_documents:
        ba_uploads.append(upload_ba_report(report_document))

    results = []
    for ba_upload in ba_uploads:
        results.append(await ba_upload)
    for result in results:
        print(result)


if __name__ == "__main__":
    created_before = datetime.now().date() + timedelta(days=1)
    threshold = created_before - timedelta(days=4)
    created_since = created_before - timedelta(days=2)
    asyncio.run(
        send_telegram_message(
            message=f"Starting SQP reports update for {created_since}- {created_before}"
        )
    )
    while created_since > threshold:
        asyncio.run(
            collect_sqp_reports(
                created_since=created_since,
                created_before=created_before,
            )
        )
        logging.debug(
            msg=f"[[REPORT]]: pushed data for {created_since} day\n[[END OF REPORT]]\n"
        )
        print(f"[[REPORT]]: pushed data for {created_since} day\n[[END OF REPORT]]\n")
        created_before, created_since = created_since, created_since - timedelta(days=1)
