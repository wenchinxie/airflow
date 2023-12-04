from sqlalchemy import TIME, Column, Integer, MetaData, String, Table, UniqueConstraint

brokers_trading = Table(
    "brokers_trading",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("broker_name", String(30)),
    Column("occured_date", TIME(timezone=True)),
    Column("stock_name", String()),
    Column("stock_code", String()),
    Column("buy_amount", Integer),
    Column("sell_amount", Integer),
    Column("buy_qty", Integer),
    Column("sell_qty", Integer),
    UniqueConstraint("broker_name", "occured_date", name="uix_data_broker"),
    extend_existing=True,
)

institutional_trading = Table(
    "institutional_investors",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("stock_name", String()),
    Column("stock_code", String()),
    Column("occured_date", TIME(timezone=True)),
    Column("ForeignInvestor", Integer),
    Column("ForeignDealer_self", Integer),
    Column("Dealer_self", Integer),
    Column("DealerHedging", Integer),
    Column("InvestmentTrust", Integer),
    UniqueConstraint(
        "stock_code", "occured_date", name="uix_data_institutional_investor"
    ),
    extend_existing=True,
)

margin_trading = Table(
    "margin_trading",
    MetaData(),
    Column("occured_date", TIME(timezone=True)),
    Column("stock_name", String()),
    Column("stock_code", String()),
    Column("MarginPurchaseBuy", Integer),
    Column("MarginPurchaseSell", Integer),
    Column("MarginPurchaseCashRepayment", Integer),
    Column("MarginPurchaseYesterdayBalance", Integer),
    Column("MarginPurchaseTodayBalance", Integer),
    Column("MarginPurchaseLimit", Integer),
    Column("ShortSaleYesterdayBalance", Integer),
    Column("ShortSaleSell", Integer),
    Column("ShortSaleBuy", Integer),
    Column("ShortSaleCashRepayment", Integer),
    Column("ShortSaleTodayBalance", Integer),
    Column("ShortSaleYesterdayBalance", Integer),
    Column("ShortSaleLimit", Integer),
    Column("OffsetLoanAndShort", Integer),
    Column("Note", String()),
    UniqueConstraint("stock_code", "occured_date", name="uix_data_margin_trading"),
    extend_existing=True,
)


def create_table_if_not_exists(table_name):
    table = eval(table_name)
    if not table.exists(engine):
        table.create(engine)
