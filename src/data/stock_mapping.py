# -*- coding: utf-8 -*-
from __future__ import annotations

"""
===================================
Stock code and name mapping
===================================

Shared stock code -> name mapping, used by analyzer, data_provider, and name_to_code_resolver.
"""

# Stock code -> name mapping (common stocks)
STOCK_NAME_MAP = {
    # === A-shares ===
    "600519": "Kweichow Moutai",
    "000001": "Ping An Bank",
    "300750": "CATL",
    "002594": "BYD",
    "600036": "China Merchants Bank",
    "601318": "Ping An of China",
    "000858": "Wuliangye",
    "600276": "Hengrui Medicine",
    "601012": "LONGi Green Energy",
    "002475": "Luxshare Precision",
    "300059": "Eastmoney",
    "002415": "Hikvision",
    "600900": "Yangtze Power",
    "601166": "Industrial Bank",
    "600028": "Sinopec",
    "600030": "CITIC Securities",
    "600031": "Sany Heavy Industry",
    "600050": "China Unicom",
    "600104": "SAIC Motor",
    "600111": "Northern Rare Earth",
    "600150": "China Shipbuilding",
    "600309": "Wanhua Chemical",
    "600406": "NARI Technology",
    "600690": "Haier Smart Home",
    "600760": "AVIC Shenyang Aircraft",
    "600809": "Shanxi Fenjiu",
    "600887": "Yili Group",
    "600930": "China Huadian New Energy",
    "601088": "China Shenhua",
    "601127": "Seres",
    "601211": "Guotai Haitong",
    "601225": "Shaanxi Coal Industry",
    "601288": "Agricultural Bank of China",
    "601328": "Bank of Communications",
    "601398": "ICBC",
    "601601": "China Pacific Insurance",
    "601628": "China Life Insurance",
    "601658": "Postal Savings Bank",
    "601668": "China State Construction",
    "601728": "China Telecom",
    "601816": "Beijing-Shanghai High-Speed Railway",
    "601857": "PetroChina",
    "601888": "China Tourism Group",
    "601899": "Zijin Mining",
    "601919": "COSCO Shipping",
    "601985": "China National Nuclear Power",
    "601988": "Bank of China",
    "603019": "ChinaData Group",
    "603259": "Wuxi AppTec",
    "603501": "Will Semiconductor",
    "603993": "Luoyang Molybdenum",
    "688008": "Montage Technology",
    "688012": "Cambricon Technologies",
    "688041": "Hygon Information Technology",
    "688111": "Kingsoft Office",
    "688256": "Cambricon",
    "688981": "Semiconductor Manufacturing International",
    # === US stocks ===
    "AAPL": "Apple",
    "TSLA": "Tesla",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet Class A",
    "GOOG": "Alphabet Class C",
    "AMZN": "Amazon",
    "NVDA": "NVIDIA",
    "META": "Meta",
    "AMD": "AMD",
    "INTC": "Intel",
    "BABA": "Alibaba",
    "PDD": "PDD Holdings",
    "JD": "JD.com",
    "BIDU": "Baidu",
    "NIO": "NIO",
    "XPEV": "XPeng Motors",
    "LI": "Li Auto",
    "COIN": "Coinbase",
    "MSTR": "MicroStrategy",
    # === Crypto pairs ===
    "BTC-USD": "Bitcoin",
    "ETH-USD": "Ethereum",
    # === HK stocks (5-digit) ===
    "00700": "Tencent Holdings",
    "03690": "Meituan",
    "01810": "Xiaomi Group",
    "09988": "Alibaba",
    "09618": "JD.com Group",
    "09888": "Baidu Group",
    "01024": "Kuaishou",
    "00981": "SMIC",
    "02015": "Li Auto",
    "09868": "XPeng Motors",
    "00005": "HSBC Holdings",
    "01299": "AIA Group",
    "00941": "China Mobile",
    "00883": "CNOOC",
}


def is_meaningful_stock_name(name: str | None, stock_code: str) -> bool:
    """Return whether a stock name is useful for display or caching."""
    if not name:
        return False

    normalized_name = str(name).strip()
    if not normalized_name:
        return False

    normalized_code = (stock_code or "").strip().upper()
    if normalized_name.upper() == normalized_code:
        return False

    if normalized_name.startswith("stock"):
        return False

    placeholder_values = {
        "N/A",
        "NA",
        "NONE",
        "NULL",
        "--",
        "-",
        "UNKNOWN",
        "TICKER",
    }
    if normalized_name.upper() in placeholder_values:
        return False

    return True
