import asyncio
import os
import tempfile

import pandas as pd
import pytz

from reports.process_reports import check_and_download_report
from reports.report_types import all_orders_report

user = os.path.expanduser("~")
user_folder = os.path.join(user, "temp")
os.makedirs(user_folder, exist_ok=True)


def main():
    days = input("How many days of sale (default = 3)?\n")
    if days != "":
        try:
            days = int(days)
        except Exception as e:
            days = input(f"Days of sale must be an integer between 1 and 60\n{e}")
    days = int(days) if days else 3

    if not days:
        report_id = input("Enter report ID (skip for new): ")
    else:
        report_id = ""

    if report_id:
        print(f"Pulling report for {report_id} id")
        all_orders_document = asyncio.run(
            check_and_download_report(report_id=report_id)
        )
    else:
        print(f"Pulling new report for {days} days back")
        all_orders_response = asyncio.run(all_orders_report(days=days))
        all_orders_document = asyncio.run(
            check_and_download_report(all_orders_response)
        )
    if not isinstance(all_orders_document, str):
        raise BaseException("Document must be a string, can't write to the file")
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, "w") as f:
        f.write(all_orders_document)

    data = pd.read_csv(f.name, sep="\t")
    data["pacific_datetime"] = (
        pd.to_datetime(data["purchase-date"], utc=True)
        .dt.tz_convert(pytz.timezone("US/Pacific"))
        .dt.tz_localize(None)
    )
    data["pacific_date"] = pd.to_datetime(data["pacific_datetime"]).dt.date

    data.to_excel(os.path.join(user_folder, "all_orders.xlsx"), index=False)
    print(f"all done, document saved to {user_folder}")


if __name__ == "__main__":
    main()
