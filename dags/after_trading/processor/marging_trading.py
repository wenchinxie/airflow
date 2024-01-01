import httpx
import pandas as pd
import pendulum
from after_trading.processor.utils import turn_year_to_ROC_year

LISTED_COL_MAPPING = {
    "代號": "stock_code",
    "名稱": "stock_name",
    "買進": "MarginPurchaseBuy",
    "賣出": "MarginPurchaseSell",
    "現金償還": "MarginPurchaseCashRepayment",
    "前日餘額": "MarginPurchaseYesterdayBalance",
    "今日餘額": "MarginPurchaseTodayBalance",
    "限額": "MarginPurchaseLimit",
    "買進.1": "ShortSaleBuy",
    "賣出.1": "ShortSaleSell",
    "現券償還": "ShortSaleCashRepayment",
    "前日餘額.1": "ShortSaleYesterdayBalance",
    "今日餘額.1": "ShortSaleTodayBalance",
    "限額.1": "ShortSaleLimit",
    "資券互抵": "OffsetLoanAndShort",
    "註記": "Note",
}

OTC_COL_MAPPING = {
    "代號": "stock_code",
    "名稱": "stock_name",
    "資買": "MarginPurchaseBuy",
    "資賣": "MarginPurchaseSell",
    "現償": "MarginPurchaseCashRepayment",
    "前資餘額(張)": "MarginPurchaseYesterdayBalance",
    "資餘額": "MarginPurchaseTodayBalance",
    "資限額": "MarginPurchaseLimit",
    "券賣": "ShortSaleSell",
    "券買": "ShortSaleBuy",
    "券償": "ShortSaleCashRepayment",
    "券餘額": "ShortSaleTodayBalance",
    "前券餘額(張)": "ShortSaleYesterdayBalance",
    "券限額": "ShortSaleLimit",
    "資券相抵(張)": "OffsetLoanAndShort",
    "備註": "Note",
}


default_date_str = pendulum.today().to_date_string()


def get_listed_margin_trading_data(orignal_date_str: str = "", listed: bool = True):
    if not orignal_date_str:
        orignal_date_str, date_str = get_date_str(listed)

    else:
        if listed:
            date_str = orignal_date_str
        else:
            date_str = turn_year_to_ROC_year(orignal_date_str)

    if listed:
        api_url = f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date_str}&selectType=ALL&response=csv"
        kwargs = {"skiprows": 7, "skipfooter": 7}

    else:
        api_url = f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=csv&d={date_str}&s=0"
        kwargs = {"skiprows": 2, "skipfooter": 20}

    with httpx.Client() as client:
        response = client.get(api_url)

    if response.status_code != 200:
        return pd.DataFrame()

    df = pd.read_csv(api_url, encoding="cp950", **kwargs)

    df["occured_date"] = orignal_date_str
    df["occured_date"] = pd.to_datetime(df["occured_date"], utc=True)
    return clean_df(df, listed)


def get_date_str(listed: bool):
    date_str = pendulum.today().to_date_string()

    if listed:
        date_str = date_str.replace("-", "")
        return date_str, date_str

    date_str = date_str.replace("-", "/")
    return date_str, turn_year_to_ROC_year(date_str)


def clean_df(df, listed):
    # clean column names
    col_mapping = LISTED_COL_MAPPING if listed else OTC_COL_MAPPING
    rename_cols = {col.strip(): col for col in df.columns}
    df = df.rename(columns=rename_cols).rename(columns=col_mapping)

    df.dropna(subset=["stock_name"], inplace=True)
    df["stock_code"].replace(r'=|"', "", regex=True, inplace=True)
    df.fillna(0, inplace=True)

    use_cols = list(col_mapping.values())
    int_cols = use_cols[2:-1]
    df[int_cols] = df[int_cols].replace(",", "", regex=True).astype(int)

    return df[use_cols + ["occured_date"]]
