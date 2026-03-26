#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock Index Generation Script

Generate stock index file for frontend autocomplete functionality
Output to apps/dsa-web/public/stocks.index.json

Two-phase strategy:
1. MVP: Use existing STOCK_NAME_MAP
2. Future: Combine with AkShare for complete list

Usage:
    python3 scripts/generate_stock_index.py
"""

import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import List, Dict, Any

# Add the project root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from pypinyin import lazy_pinyin
    PYPINYIN_AVAILABLE = True
except ImportError:
    PYPINYIN_AVAILABLE = False
    print("[Warning] pypinyin not available, pinyin fields will be empty")
    print("[Info] Install with: pip install pypinyin")


def normalize_name_for_pinyin(name: str) -> str:
    """
    Normalize stock name to avoid special prefixes and full-width characters polluting pinyin index

    Args:
        name: Original stock name

    Returns:
        Normalized name for pinyin generation
    """
    normalized = unicodedata.normalize('NFKC', name).strip()

    # Strip common A-share prefixes while preserving the core name.
    normalized = re.sub(r'^(?:\*?ST|N)+', '', normalized, flags=re.IGNORECASE)

    return normalized.strip() or unicodedata.normalize('NFKC', name).strip()


def generate_stock_index_from_map() -> List[Dict[str, Any]]:
    """
    Generate index from STOCK_NAME_MAP (MVP)

    Returns:
        List of stock index
    """
    from src.data.stock_mapping import STOCK_NAME_MAP

    index = []

    for code, name in STOCK_NAME_MAP.items():
        # Generate pinyin fields.
        pinyin_full = None
        pinyin_abbr = None
        if PYPINYIN_AVAILABLE:
            try:
                normalized_name = normalize_name_for_pinyin(name)
                py = lazy_pinyin(normalized_name)
                pinyin_full = ''.join(py)
                pinyin_abbr = ''.join([p[0] for p in py])
            except Exception:
                pass

        # Determine market and asset type.
        market, asset_type = determine_market_and_type(code)

        # Generate short aliases.
        aliases = generate_aliases(name)

        index.append({
            "canonicalCode": build_canonical_code(code, market),
            "displayCode": code,
            "nameZh": name,
            "pinyinFull": pinyin_full,
            "pinyinAbbr": pinyin_abbr,
            "aliases": aliases,
            "market": market,
            "assetType": asset_type,
            "active": True,
            "popularity": 100,  # Default popularity
        })

    return index


def determine_market_and_type(code: str) -> tuple:
    """
    Determine market and asset type based on stock code

    Args:
        code: Stock code

    Returns:
        Tuple of (market, asset_type)
    """
    if code.isdigit():
        if len(code) == 5:
            # Five digits: likely HK stock or legacy B-share.
            if code.startswith('0') or code.startswith('2'):
                return 'HK', 'stock'
            return 'CN', 'stock'
        elif len(code) == 6:
            # Six digits: A-share universe.
            if code.startswith('6'):
                return 'CN', 'stock'  # Shanghai
            elif code.startswith(('0', '2', '3')):
                return 'CN', 'stock'  # Shenzhen
            elif code.startswith('8'):
                return 'BSE', 'stock'  # Beijing Stock Exchange
            return 'CN', 'stock'
        elif len(code) == 4:
            # Four digits: likely a US symbol or special market code.
            return 'US', 'stock'

    # characterparentcode，US stockorother
    return 'US', 'stock'


def market_to_suffix(market: str) -> str:
    """
    Convert market code to suffix

    Args:
        market: Market code

    Returns:
        Market suffix
    """
    suffix_map = {
        'CN': 'SH',  # simplifyprocessing，defaultShanghai
        'HK': 'HK',
        'US': 'US',
        'INDEX': 'SH',
        'ETF': 'SH',
        'BSE': 'BJ',
    }
    return suffix_map.get(market, 'SH')


def build_canonical_code(code: str, market: str) -> str:
    """
    Generate canonical stock code based on code and market.

    A-shares need to distinguish between SH/SZ/BJ, cannot rely solely on the general CN -> SH mapping.
    """
    if market == 'CN' and code.isdigit() and len(code) == 6:
        # Shanghai Stock Exchange (SH)
        # 60xxxx: Main board, 688xxx: STAR market, 900xxx: B-shares
        if code.startswith(('6', '900')):
            return f"{code}.SH"
        
        # Shenzhen Stock Exchange (SZ)
        # 00xxxx: Main board, 30xxxx: ChiNext, 20xxxx: B-shares
        if code.startswith(('0', '2', '3')):
            return f"{code}.SZ"

        # Beijing Stock Exchange (BJ)
        # 920xxx: New codes and migrated stock codes after April 2024
        # 43xxxx, 83xxxx, 87xxxx, 88xxxx: Historical/Temporary codes
        # 81xxxx, 82xxxx: Convertible bonds/Preferred stocks
        if code.startswith(('920', '43', '83', '87', '88', '81', '82')):
            return f"{code}.BJ"

    if market == 'BSE' and code.isdigit() and len(code) == 6:
        return f"{code}.BJ"

    return f"{code}.{market_to_suffix(market)}"


def generate_aliases(name: str) -> List[str]:
    """
    Generate stock aliases (abbreviations)

    Args:
        name: Full stock name

    Returns:
        List of aliases
    """
    aliases = []

    # normallyseeabbreviationmapping
    alias_map = {
        'Kweichow Moutai': ['Maotai'],
        'Ping An of China': ['Ping An'],
        'Ping An Bank': ['Ping An Bank'],
        'China Merchants Bank': ['inviterow'],
        'Wuliangye': ['Wuliang'],
        'CATL': ['Ningde'],
        'BYD': ['Biya'],
        'ICBC': ['Industrialrow'],
        'Constructionbank': ['Constructionrow'],
        'Agricultural Bank': ['Agriculturalrow'],
        'Bank of China': ['inrow'],
        'Bank of Communications': ['exchangerow'],
        'Industrial Bank': ['Industrial'],
        'SPDB': ['Pudongsend'],
        'Minshengbank': ['Minsheng'],
        'CITIC Securities': ['CITIC'],
        'Eastmoney': ['Eastmoney'],
        'Hikvision': ['Hikvision'],
        'LONGi Green Energy': ['Longbase'],
        'China Shenhua': ['Shenhua'],
        'Yangtze Power': ['longElectric'],
        'Sinopec': ['Petrochemical'],
        'PetroChina': ['Petroleum'],
    }

    if name in alias_map:
        aliases.extend(alias_map[name])

    return aliases


def compress_index(index: List[Dict[str, Any]]) -> List[List]:
    """
    Compress index to array format to reduce file size

    Args:
        index: Original index

    Returns:
        Compressed index
    """
    compressed = []
    for item in index:
        compressed.append([
            item["canonicalCode"],
            item["displayCode"],
            item["nameZh"],
            item.get("pinyinFull"),
            item.get("pinyinAbbr"),
            item.get("aliases", []),
            item["market"],
            item["assetType"],
            item["active"],
            item.get("popularity", 0),
        ])
    return compressed


def main():
    """Main function"""
    print("startinggeneratingstockindex...")

    # generatingindex（MVP：usecurrenthasmapping）
    index = generate_stock_index_from_map()
    print(f"totalgenerating {len(index)} itemsindex")

    # outputpath
    output_path = Path(__file__).parent.parent / "apps" / "dsa-web" / "public" / "stocks.index.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # compressionformat（reducefilesize）
    compressed = compress_index(index)

    # writingfile
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(compressed, f, ensure_ascii=False, separators=(',', ':'))

    file_size = output_path.stat().st_size
    print(f"indexalreadygenerating：{output_path}")
    print(f"filesize：{file_size / 1024:.2f} KB")

    # verificationfilecanread
    with open(output_path, 'r', encoding='utf-8') as f:
        test_data = json.load(f)
        print(f"verificationvia：{len(test_data)} itemsrecord")

    return 0


if __name__ == "__main__":
    sys.exit(main())
