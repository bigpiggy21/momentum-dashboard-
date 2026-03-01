"""
Ticker universe mapping: thematic groups, US underlyings, LSE leveraged equivalents.
"""

TICKER_UNIVERSE = {
    "VOLATILITY": {
        "tickers": {
            "VIX": {"us": "VIX", "lse_long": ["SVLT"], "lse_short": ["LTSV", "VILX"]},
        }
    },
    "S&P 500": {
        "tickers": {
            "SPX": {"us": "SPY", "lse_long": ["5SPY", "5LUS", "3SPY", "3LUS"], "lse_short": ["SSPY", "5ULS", "3ULS"]},
        }
    },
    "NDX 100": {
        "tickers": {
            "QQQ": {"us": "QQQ", "lse_long": ["5QQQ", "LQS5", "LQQ3"], "lse_short": ["SQQQ", "SQS5", "LQQS"]},
        }
    },
    "US BONDS": {
        "tickers": {
            "TLT": {"us": "TLT", "lse_long": ["5TLT"], "lse_short": ["TLT5", "5STL"]},
            "IEF": {"us": "IEF", "lse_long": ["IEF5"], "lse_short": ["5SIE"]},
            "TIP": {"us": "TIP", "lse_long": ["5TIP"], "lse_short": ["5STI"]},
        }
    },
    "INDEX LEVERAGES": {
        "tickers": {
            "SOXX": {"us": "SOXX", "lse_long": ["SOX4", "3SEM"], "lse_short": ["SX4S", "3SMC"]},
            "ARKK": {"us": "ARKK", "lse_long": ["3ARK"], "lse_short": ["SARK"]},
        }
    },
    "SEMI INDIVIDUALS": {
        "tickers": {
            "NVDA": {"us": "NVDA", "lse_long": ["NVD3"], "lse_short": ["SNV3"]},
            "TSM": {"us": "TSM", "lse_long": ["TSM3"], "lse_short": ["STSM"]},
            "AMD": {"us": "AMD", "lse_long": ["AMD3"], "lse_short": ["SAMD"]},
            "MU": {"us": "MU", "lse_long": ["2MU"], "lse_short": []},
            "SMCI": {"us": "SMCI", "lse_long": ["SMCI"], "lse_short": []},
        }
    },
    "MAG 6": {
        "tickers": {
            "AAPL": {"us": "AAPL", "lse_long": ["3AAP", "3LWP"], "lse_short": ["3SAA", "3SWP"]},
            "AMZN": {"us": "AMZN", "lse_long": ["3AMZ", "3LZP"], "lse_short": ["3SAM", "3SZP"]},
            "GOOG": {"us": "GOOG", "lse_long": ["3LAL", "3GOO", "3LGP"], "lse_short": ["3SGG", "3SGP"]},
            "NFLX": {"us": "NFLX", "lse_long": ["3NFL", "3LNP"], "lse_short": ["3SNP"]},
            "MSFT": {"us": "MSFT", "lse_long": ["3MSF", "3LMP"], "lse_short": ["3SMF", "3SMP"]},
            "META": {"us": "META", "lse_long": ["3FB", "3LFP"], "lse_short": ["SFB3", "3SFP"]},
            "TSLA": {"us": "TSLA", "lse_long": ["3TSL", "3LTP"], "lse_short": ["TSLQ", "3STP"]},
        }
    },
    "OTHER US TECH": {
        "tickers": {
            "PLTR": {"us": "PLTR", "lse_long": ["PAL3"], "lse_short": ["SPL3"]},
            "CRM": {"us": "CRM", "lse_long": ["3CRM"], "lse_short": []},
            "ABNB": {"us": "ABNB", "lse_long": ["3ABN"], "lse_short": []},
            "UBER": {"us": "UBER", "lse_long": ["3LUP"], "lse_short": []},
            "PYPL": {"us": "PYPL", "lse_long": ["3PYP"], "lse_short": ["SPYP"]},
            "SPOT": {"us": "SPOT", "lse_long": ["LPO3"], "lse_short": []},
        }
    },
    "CRYPTO": {
        "tickers": {
            "COIN": {"us": "COIN", "lse_long": ["3CON", "LCO3"], "lse_short": ["S3CO"]},
            "MSTR": {"us": "MSTR", "lse_long": ["3MST", "LMI3"], "lse_short": ["SMST", "SMI3"]},
            "XYZ": {"us": "XYZ", "lse_long": ["3SQ", "LSQ3"], "lse_short": ["SSQ3"]},
        }
    },
    "WATCHLIST": {
        "tickers": {
            "ASML": {"us": "ASML", "lse_long": ["3ASM"], "lse_short": ["SASL"]},
            "AVGO": {"us": "AVGO", "lse_long": ["3AVG"], "lse_short": ["SAVG"]},
            "FUTU": {"us": "FUTU", "lse_long": ["3FUT"], "lse_short": []},
            "LLY": {"us": "LLY", "lse_long": ["3LLY"], "lse_short": ["SLLY"]},
            "HIMS": {"us": "HIMS", "lse_long": ["3HIM"], "lse_short": []},
            "INTC": {"us": "INTC", "lse_long": ["3INT"], "lse_short": []},
            "HOOD": {"us": "HOOD", "lse_long": ["3HOD"], "lse_short": []},
            "UNH": {"us": "UNH", "lse_long": ["3UNH"], "lse_short": []},
        }
    },
    "US OTHER": {
        "tickers": {
            "BA": {"us": "BA", "lse_long": ["3BA"], "lse_short": ["SBA"]},
            "BRK.B": {"us": "BRK.B", "lse_long": ["2BRK"], "lse_short": []},
            "RACE": {"us": "RACE", "lse_long": ["3RAC"], "lse_short": ["3SRA"]},
            "DIS": {"us": "DIS", "lse_long": ["3DIS"], "lse_short": ["SDIS"]},
            "V": {"us": "V", "lse_long": ["2VIS"], "lse_short": []},
            "MRNA": {"us": "MRNA", "lse_long": ["3MRN", "MOL3"], "lse_short": ["SOL3"]},
        }
    },
    "FINANCIALS": {
        "tickers": {
            "XLF": {"us": "XLF", "lse_long": ["3XLF"], "lse_short": ["3SXL"]},
            "GS": {"us": "GS", "lse_long": ["2GS"], "lse_short": ["SGSE"]},
            "UBS": {"us": "UBS", "lse_long": ["UBS3"], "lse_short": ["3SUS"]},
        }
    },
    "EU INDEXES": {
        "tickers": {
            "STOXX50": {"us": "STOXX50", "lse_long": [], "lse_short": []},
            "UKX": {"us": "UKX", "lse_long": ["3UKL"], "lse_short": ["3UKS"]},
            "DAX": {"us": "DAX", "lse_long": ["3DAX", "3DEL"], "lse_short": ["SDAX", "3SDE"]},
        }
    },
    "CHINA INDIVIDUALS": {
        "tickers": {
            "BABA": {"us": "BABA", "lse_long": ["BAB3"], "lse_short": ["SBA3"]},
            "JD": {"us": "JD", "lse_long": ["JD3"], "lse_short": ["SJD"]},
            "BIDU": {"us": "BIDU", "lse_long": ["3BID"], "lse_short": ["SBIU"]},
            "NIO": {"us": "NIO", "lse_long": ["3NIO", "3LIP"], "lse_short": ["3SIP"]},
        }
    },
    "COMMODITIES - ENERGY": {
        "tickers": {
            "OIL": {"us": "USO", "lse_long": ["3LOI", "2OIL"], "lse_short": ["3SOI", "SOIE"]},
            "NATGAS": {"us": "UNG", "lse_long": ["3NGL", "3LGS", "3LNG"], "lse_short": ["3NGS"]},
        }
    },
    "COMMODITIES - PRECIOUS METALS": {
        "tickers": {
            "GOLD": {"us": "GLD", "lse_long": ["3GLD", "3LGO"], "lse_short": ["SGOL", "3SGO"]},
            "SILVER": {"us": "SLV", "lse_long": ["3SLV", "3SIL"], "lse_short": ["3SSI"]},
        }
    },
    "COMMODITIES - MINERS": {
        "tickers": {
            "GDX": {"us": "GDX", "lse_long": ["GDX3"], "lse_short": ["SGDX"]},
        }
    },
    "COMMODITIES - OTHER METALS": {
        "tickers": {
            "COPPER": {"us": "CPER", "lse_long": ["3HCL"], "lse_short": ["3HCS"]},
        }
    },
    "COMMODITY STOCKS": {
        "tickers": {
            "RIO": {"us": "RIO", "lse_long": ["3LRI"], "lse_short": ["3SRI"]},
            "BP": {"us": "BP", "lse_long": ["3BP"], "lse_short": ["3SBP"]},
            "XOM": {"us": "XOM", "lse_long": ["3XOM"], "lse_short": ["3SXO"]},
        }
    },
    "CURRENCIES": {
        "tickers": {
            "JPY/USD": {"us": "FXY", "lse_long": ["LJP3"], "lse_short": ["SJP3"]},
            "EUR/USD": {"us": "FXE", "lse_long": [], "lse_short": []},
        }
    },
}


def get_all_us_tickers() -> list:
    """Get flat list of all US underlying tickers."""
    tickers = []
    for group_name, group_data in TICKER_UNIVERSE.items():
        for ticker_name, ticker_info in group_data['tickers'].items():
            tickers.append(ticker_info['us'])
    return tickers


def get_prototype_tickers() -> dict:
    """Get just the Semi Individuals group for prototyping."""
    return {"SEMI INDIVIDUALS": TICKER_UNIVERSE["SEMI INDIVIDUALS"]}


def get_all_groups() -> list:
    """Get ordered list of group names."""
    return list(TICKER_UNIVERSE.keys())
