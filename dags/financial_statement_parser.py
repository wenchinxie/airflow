import pendulum
from airflow import DAG


with DAG(
    "ParseFinancialStatement",
    schedule_interval="@once",
    start_date=pendulum.datetime(2023, 1, 12, 9, 0, tz="Asia/Taipei"),
    catchup=False,
    tags=["quarterly financial statement"],
) as dag:
    import re
    import os
    from airflow.operators.python import ExternalPythonOperator
    from airflow.configuration import conf

    def pdftoText(target_folder: str):
        import pdfium

        for root, _, files in os.walk(target_folder):
            for file in files:
                file_path = os.path.join(root, file)
                pdf = pdfium.PdfDocument(file_path)
                page_start, page_end = get_target_page_range(pdf)
                convertPDFtoText(file_path, page_start, page_end)

    def get_target_page_range(pdf):
        description = r"(?<=重要會計項目之說明).*\d{1,}.*\d{1,}"
        for page in range(0, len(pdf)):
            content = pdf[page].get_textpage().get_text_range()
            res = re.findall(description, content)
            if len(res) != 0:
                matches = re.findall(r"\d{1,}", res[0])

                return int(matches[0]) - 1, int(matches[1]) - 1  # offset for pdf pages

        raise ValueError("No table of contents found.")

    def convertPDFtoText(filename: str, page_start: int, page_end):
        import subprocess

        cmd = [
            "node",
            "/mnt/c/Users/s3309/FinancialSummarizer/src/FniancialStatementsParser/main.js",
            filename,
            page_start,
            page_end,
        ]

    save_inds_comps_to_db = ExternalPythonOperator(
        task_id="save_industries_info",
        python=conf.get("core", "virtualenv"),
        python_callable=call_inds_scraper,
    )

    save_inds_comps_to_db
