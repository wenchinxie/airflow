import os
from datetime import datetime, date
import shutil
import pandas as pd


def get_investor_type_and_date(file_name):
    name_split = file_name.split("_")
    return name_split[0][-3:], pd.to_datetime(
        name_split[1].split(".")[0], format="%Y%m%d"
    )


def get_rename_cols_and_skiprows(investor_type):
    print(investor_type)
    common_dict = {
        "證券代號": "stock_code",
        "證券名稱": "stock_name",
    }

    if investor_type == "38U":
        common_dict.update(
            {"買賣超股數": "ForeignInvestor", "買賣超股數.1": "ForeignDealer_self"}
        )
        return common_dict, 2
    if investor_type == "43U":
        common_dict.update({"買賣超股數": "Dealer_self", "買賣超股數.1": "DealerHedging"})
        return common_dict, 2
    if investor_type == "44U":
        common_dict.update({"買賣超股數": "InvestmentTrust"})
        return common_dict, 1

    common_dict = {
        "資料日期": "date",
        "代號": "stock_id",
        "名稱": "stock_name",
        "外資及陸資不含外資自營商買賣超股數": "ForeignInvestor",
        "外資自營商買賣超股數": "ForeignDealer_self",
        "投信買賣超股數": "InvestmentTrust",
        "自營商自行買賣買賣超股數": "Dealer_self",
        "自營商避險買賣超股數": "DealerHedging",
    }
    return common_dict, None


def clean_df(df, renamed_cols_mapping):
    renamed_cols = list(renamed_cols_mapping.values())
    df.rename(columns=renamed_cols_mapping, inplace=True)
    df = df.dropna(subset=["stock_name"])
    df["stock_id"] = df["stock_id"].replace(r"\D", "", regex=True)

    cleaned_cols = [
        col for col in renamed_cols if col not in ("stock_id", "stock_name", "date")
    ]
    df[cleaned_cols] = df[cleaned_cols].replace(r",", "", regex=True).astype(int)

    return df[renamed_cols]


def get_institutional_investor_df(download_foler, file_name):
    investor_type, occurred_date = get_investor_type_and_date(file_name)
    rename_cols_mapping, skiprows = get_rename_cols_and_skiprows(investor_type)

    df = pd.read_csv(
        os.path.join(download_foler, file_name),
        encoding="big5hkscs",
        skiprows=skiprows,
    )
    cleaned_df = clean_df(df, rename_cols_mapping)
    cleaned_df["date"] = occurred_date
    return cleaned_df


def get_file_last_modified_date(file_path):
    last_modified_timestamp = os.path.getmtime(file_path)
    return datetime.fromtimestamp(last_modified_timestamp).date()


def get_concat_listed_ii_df(download_folder: str, data_lake: str):
    today = date.today()
    all_data = pd.DataFrame()

    all_files = os.listdir(download_folder)
    for file_name in all_files:
        if not (file_name.endswith(".csv") and file_name.startswith("TWT")):
            continue
        file_path = os.path.join(download_folder, file_name)
        last_modified_date = get_file_last_modified_date(file_path)
        if last_modified_date == today:
            df = get_institutional_investor_df(download_folder, file_name)
            if all_data.empty:
                all_data = df
            else:
                all_data = all_data.merge(
                    df, on=["stock_name", "stock_id", "date"], how="left"
                )

        move_to = os.path.join(
            data_lake + "/after_trading/instituional_investors", file_name
        )
        shutil.move(file_path, move_to)
    return all_data.fillna(0)


def get_otc_ii_df(file_path):
    renamed_cols_mapping, _ = get_rename_cols_and_skiprows("otc")
    df = pd.read_csv(file_path)
    return clean_df(df, renamed_cols_mapping)
