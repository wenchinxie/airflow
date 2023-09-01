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

    target_folder = conf.get("investment_folder", "qrt_folder")
    tools_folder = conf.get("js_tools", "tools_folder")

    def pdf_to_text(target_folder: str, tools_folder: str):
        import os
        import re
        import shutil

        import pypdfium2 as pdfium

        def get_target_page_range(pdf):
            description = r"(?<=重要會計項目之說明).*\d{1,}.*\d{1,}"
            for page in range(0, len(pdf)):
                content = pdf[page].get_textpage().get_text_range()
                res = re.findall(description, content)
                if len(res) != 0:
                    matches = re.findall(r"\d{1,}", res[0])

                    return (
                        matches[0],
                        matches[1],
                    )  # offset for pdf pages

            raise ValueError("No table of contents found.")

        def convert_pdf_to_text(
            tools_folder: str, filename: str, page_start: int, page_end
        ):
            import subprocess

            cmd = [
                "node",
                f"{tools_folder}convertPDFtoText.js",
                filename,
                page_start,
                page_end,
            ]
            subprocess.check_call(cmd)

        def move_done_pdf(file_path: str, complete: bool = True):
            if complete:
                txt_path = re.sub("pdf$", "txt", file_path)
                shutil.move(file_path, re.sub("to_convert", "completed", file_path))
                shutil.move(txt_path, re.sub("to_convert", "to_parse", txt_path))
            else:
                shutil.move(file_path, re.sub("to_convert", "problem", file_path))

        folder_to_convert = os.path.join(target_folder, "to_convert")
        print("folder_to_convert---", folder_to_convert)
        for root, _, files in os.walk(folder_to_convert):
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith(".pdf"):
                    pdf = pdfium.PdfDocument(file_path)
                    try:
                        page_start, page_end = get_target_page_range(pdf)
                        print("handle---", file_path)
                        convert_pdf_to_text(
                            tools_folder, file_path, page_start, page_end
                        )
                        move_done_pdf(file_path)
                    except Exception as e:
                        print("exception---", e)
                        move_done_pdf(file_path, False)

    save_inds_comps_to_db = ExternalPythonOperator(
        task_id="save_industries_info",
        python=conf.get("venv", "financialparser"),
        python_callable=pdf_to_text,
        op_args=[target_folder, tools_folder],
    )

    save_inds_comps_to_db
