"""
Priority - Expanded watchlist.
Each underlying from Priority plus all US-listed leveraged/inverse ETFs.
Auto-generated from Polygon reference data (2026-03-11).
"""

PRIORITY_EXPANDED_GROUPS = [
    ("S&P 500", [
        ("SPY", "SPY", "stock"),
        ("SDS", "SDS", "stock"),  # ProShares UltraShort S&P500
        ("SPDN", "SPDN", "stock"),  # Direxion Daily S&P 500 Bear 1X ETF
        ("SPUU", "SPUU", "stock"),  # Direxion Daily S&P 500 Bull 2X ETF
        ("SPXL", "SPXL", "stock"),  # Direxion Daily S&P 500  Bull 3x ETF
        ("SPXS", "SPXS", "stock"),  # Direxion Daily S&P 500 Bear 3x ETF
        ("SPXU", "SPXU", "stock"),  # ProShares UltraPro Short S&P 500
        ("SPYQ", "SPYQ", "stock"),  # Tradr 2X Long SPY Quarterly ETF
        ("SSO", "SSO", "stock"),  # ProShares Ultra S&P500
        ("UPRO", "UPRO", "stock"),  # ProShares UltraPro S&P 500
    ]),
    ("NDX 100", [
        ("QQQ", "QQQ", "stock"),
        ("MQQQ", "MQQQ", "stock"),  # Investment Managers Series Trust II Tradr 2X Long Innovation
        ("QID", "QID", "stock"),  # ProShares UltraShort QQQ
        ("QLD", "QLD", "stock"),  # ProShares Ultra QQQ
        ("QQDN", "QQDN", "stock"),  # ProShares Trust ProShares UltraShort QQQ Mega
        ("QQQP", "QQQP", "stock"),  # Investment Managers Series Trust II Tradr 2X Long Innovation
        ("QQUP", "QQUP", "stock"),  # ProShares Trust ProShares Ultra QQQ Mega
        ("SQQQ", "SQQQ", "stock"),  # ProShares UltraPro Short QQQ
        ("TQQQ", "TQQQ", "stock"),  # ProShares  UltraPro QQQ
    ]),
    ("Sectors", [
        ("TLT", "TLT", "stock"),
        ("TBT", "TBT", "stock"),  # ProShares Trust UltraShort Lehman 20+ Year Treasury
        ("TMF", "TMF", "stock"),  # Direxion Daily 20+ Year Treasury Bull 3X ETF
        ("TMV", "TMV", "stock"),  # Direxion Daily 20+ Year Treasury Bear 3X ETF
        ("TTT", "TTT", "stock"),  # ProShares UltraPro Short 20+ Year Treasury
        ("UBT", "UBT", "stock"),  # ProShares Ultra 20+ Year Treasury
        ("IEF", "IEF", "stock"),
        ("PST", "PST", "stock"),  # ProShares Trust UltraShort Lehman 7-10 Year Treasury
        ("TYD", "TYD", "stock"),  # Direxion Daily 7-10 Year Treasury Bull 3X ETF
        ("TYO", "TYO", "stock"),  # Direxion Daily 7-10 Year Treasury Bear 3X ETF
        ("UST", "UST", "stock"),  # ProShares Ultra 7-10 Year Treasury
        ("SMH", "SMH", "stock"),
        ("SOXL", "SOXL", "stock"),  # Direxion Daily Semiconductor Bull 3X ETF
        ("SOXS", "SOXS", "stock"),  # Direxion Daily Semiconductor Bear 3X ETF
        ("SSG", "SSG", "stock"),  # ProShares UltraShort Semiconductors
        ("TSXD", "TSXD", "stock"),  # Direxion Daily Semiconductors Top 5 Bear 2X ETF
        ("TSXU", "TSXU", "stock"),  # Direxion Daily Semiconductors Top 5 Bull 2X ETF
        ("USD", "USD", "stock"),  # ProShares Ultra Semiconductors
        ("XLE", "XLE", "stock"),
        ("DRIP", "DRIP", "stock"),  # Direxion Daily S&P Oil & Gas Exp. & Prod. Bear 2X ETF
        ("ERX", "ERX", "stock"),  # Direxion Daily Energy Bull 2X ETF
        ("ERY", "ERY", "stock"),  # Direxion Daily Energy Bear 2X ETF
        ("GUSH", "GUSH", "stock"),  # Direxion Daily S&P Oil & Gas Exp. & Prod. Bull 2X ETF
        ("XLF", "XLF", "stock"),
        ("FAS", "FAS", "stock"),  # Direxion Daily Financial Bull 3x ETF
        ("FAZ", "FAZ", "stock"),  # Direxion Daily Financial Bear 3x ETF
        ("SKF", "SKF", "stock"),  # ProShares UltraShort Financials
        ("UYG", "UYG", "stock"),  # ProShares Ultra Financials
    ]),
    ("Semis", [
        ("NVDA", "NVDA", "stock"),
        ("NVD", "NVD", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Short NVDA Daily ET
        ("NVDD", "NVDD", "stock"),  # Direxion Shares ETF Trust Direxion Daily NVDA Bear 1X ETF
        ("NVDG", "NVDG", "stock"),  # Leverage Shares 2X Long NVDA Daily ETF
        ("NVDL", "NVDL", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Long NVDA Daily ETF
        ("NVDO", "NVDO", "stock"),  # Leverage Shares 2x Capped Accelerated NVDA Monthly ETF
        ("NVDQ", "NVDQ", "stock"),  # T-Rex 2X Inverse NVIDIA Daily Target ETF
        ("NVDS", "NVDS", "stock"),  # Investment Managers Series Trust II Tradr 1.5X Short NVDA Da
        ("NVDU", "NVDU", "stock"),  # Direxion Shares ETF Trust Direxion Daily NVDA Bull 2X ETF
        ("NVDX", "NVDX", "stock"),  # T-Rex 2X Long NVIDIA Daily Target ETF
        ("AVGO", "AVGO", "stock"),
        ("AVGG", "AVGG", "stock"),  # Leverage Shares 2X Long AVGO Daily ETF
        ("AVGU", "AVGU", "stock"),  # GraniteShares 2x Long AVGO Daily ETF
        ("AVGX", "AVGX", "stock"),  # Defiance Daily Target 2X Long AVGO ETF
        ("AVL", "AVL", "stock"),  # Direxion Shares ETF Trust Direxion Daily AVGO Bull 2X ETF
        ("AVS", "AVS", "stock"),  # Direxion Shares ETF Trust Direxion Daily AVGO Bear 1X ETF
        ("TSM", "TSM", "stock"),
        ("TSMG", "TSMG", "stock"),  # Leverage Shares 2X Long TSM Daily ETF
        ("TSMU", "TSMU", "stock"),  # GraniteShares 2x Long TSM Daily ETF
        ("TSMX", "TSMX", "stock"),  # Direxion Shares ETF Trust Direxion Daily TSM Bull 2X ETF
        ("TSMZ", "TSMZ", "stock"),  # Direxion Shares ETF Trust Direxion Daily TSM Bear 1X ETF
        ("AMD", "AMD", "stock"),
        ("AMDD", "AMDD", "stock"),  # Direxion Shares ETF Trust Direxion Daily AMD Bear 1X ETF
        ("AMDG", "AMDG", "stock"),  # Leverage Shares 2X Long AMD Daily ETF
        ("AMDL", "AMDL", "stock"),  # GraniteShares 2x Long AMD Daily ETF
        ("AMUU", "AMUU", "stock"),  # Direxion Shares ETF Trust Direxion Daily AMD Bull 2X ETF
        ("MU", "MU", "stock"),
        ("MUD", "MUD", "stock"),  # Direxion Shares ETF Trust Direxion Daily MU Bear 1X ETF
        ("MULL", "MULL", "stock"),  # GraniteShares 2x Long MU Daily ETF
        ("MUU", "MUU", "stock"),  # Direxion Shares ETF Trust Direxion Daily MU Bull 2X ETF
        ("SMCI", "SMCI", "stock"),
        ("SMCL", "SMCL", "stock"),  # GraniteShares 2x Long SMCI Daily ETF
        ("SMCX", "SMCX", "stock"),  # Defiance Daily Target 2X Long SMCI ETF
        ("SMCZ", "SMCZ", "stock"),  # Defiance Daily Target 2X Short SMCI ETF
        ("ASML", "ASML", "stock"),
        ("ASMG", "ASMG", "stock"),  # Leverage Shares 2X Long ASML Daily ETF
        ("ASMU", "ASMU", "stock"),  # Direxion Daily ASML Bull 2X ETF
        ("INTC", "INTC", "stock"),
        ("INTW", "INTW", "stock"),  # GraniteShares 2x Long INTC Daily ETF
        ("LINT", "LINT", "stock"),  # Direxion Daily INTC Bull 2X ETF
    ]),
    ("Mags", [
        ("AAPL", "AAPL", "stock"),
        ("AAPB", "AAPB", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Long AAPL Daily ETF
        ("AAPD", "AAPD", "stock"),  # Direxion Shares ETF Trust Direxion Daily AAPL Bear 1X ETF
        ("AAPU", "AAPU", "stock"),  # Direxion Shares ETF Trust Direxion Daily AAPL Bull 2X ETF
        ("AAPX", "AAPX", "stock"),  # T-Rex 2X Long Apple Daily Target ETF
        ("AMZN", "AMZN", "stock"),
        ("AMZD", "AMZD", "stock"),  # Direxion Shares ETF Trust Direxion Daily AMZN Bear 1X ETF
        ("AMZU", "AMZU", "stock"),  # Direxion Shares ETF Trust Direxion Daily AMZN Bull 2X ETF
        ("AMZZ", "AMZZ", "stock"),  # GraniteShares 2x Long AMZN Daily ETF
        ("GOOG", "GOOG", "stock"),
        ("GGLL", "GGLL", "stock"),  # Direxion Shares ETF Trust Direxion Daily GOOGL Bull 2X ETF
        ("GGLS", "GGLS", "stock"),  # Direxion Shares ETF Trust Direxion Daily GOOGL Bear 1X ETF
        ("GOOX", "GOOX", "stock"),  # T-Rex 2X Long Alphabet Daily Target ETF
        ("GOU", "GOU", "stock"),  # GraniteShares 2x Long GOOGL Daily ETF
        ("NFLX", "NFLX", "stock"),
        ("NFLU", "NFLU", "stock"),  # T-Rex 2X Long NFLX Daily Target ETF
        ("NFXL", "NFXL", "stock"),  # Direxion Shares ETF Trust Direxion Daily NFLX Bull 2X ETF
        ("NFXS", "NFXS", "stock"),  # Direxion Shares ETF Trust Direxion Daily NFLX Bear 1X ETF
        ("MSFT", "MSFT", "stock"),
        ("MSFD", "MSFD", "stock"),  # Direxion Shares ETF Trust Direxion Daily MSFT Bear 1X ETF
        ("MSFL", "MSFL", "stock"),  # GraniteShares 2x Long MSFT Daily ETF
        ("MSFU", "MSFU", "stock"),  # Direxion Shares ETF Trust Direxion Daily MSFT Bull 2X ETF
        ("MSFX", "MSFX", "stock"),  # T-Rex 2X Long Microsoft Daily Target ETF
        ("META", "META", "stock"),
        ("FBL", "FBL", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Long META Daily ETF
        ("METD", "METD", "stock"),  # Direxion Shares ETF Trust Direxion Daily META Bear 1X ETF
        ("METU", "METU", "stock"),  # Direxion Shares ETF Trust Direxion Daily META Bull 2X ETF
        ("TSLA", "TSLA", "stock"),
        ("TSDD", "TSDD", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Short TSLA Daily ET
        ("TSL", "TSL", "stock"),  # GraniteShares 1.25x Long TSLA  Daily ETF
        ("TSLG", "TSLG", "stock"),  # Leverage Shares 2X Long TSLA Daily ETF
        ("TSLL", "TSLL", "stock"),  # Direxion Shares ETF Trust Direxion Daily TSLA Bull 2X ETF
        ("TSLO", "TSLO", "stock"),  # Leverage Shares 2x Capped Accelerated TSLA Monthly ETF
        ("TSLQ", "TSLQ", "stock"),  # Investment Managers Series Trust II Tradr 2X Short TSLA Dail
        ("TSLR", "TSLR", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Long TSLA Daily ETF
        ("TSLS", "TSLS", "stock"),  # Direxion Shares ETF Trust Direxion Daily TSLA Bear 1X ETF
        ("TSLT", "TSLT", "stock"),  # T-REX 2X Long Tesla Daily Target ETF
        ("TSLZ", "TSLZ", "stock"),  # T-Rex 2X Inverse Tesla Daily Target ETF
        ("PLTR", "PLTR", "stock"),
        ("PLOO", "PLOO", "stock"),  # Leverage Shares 2x Capped Accelerated PLTR Monthly ETF
        ("PLTD", "PLTD", "stock"),  # Direxion Shares ETF Trust Direxion Dailly PLTR Bear 1X ETF
        ("PLTG", "PLTG", "stock"),  # Leverage Shares 2X Long PLTR Daily ETF
        ("PLTU", "PLTU", "stock"),  # Direxion Shares ETF Trust Direxion Daily PLTR Bull 2X ETF
        ("PLTZ", "PLTZ", "stock"),  # Defiance Daily Target 2x Short PLTR ETF
        ("PTIR", "PTIR", "stock"),  # GraniteShares 2x Long PLTR Daily ETF
        ("ARKK", "ARKK", "stock"),
    ]),
    ("Crypto", [
        ("BTC", "X:BTCUSD", "crypto"),
        ("BITU", "BITU", "stock"),  # ProShares Ultra Bitcoin ETF
        ("BITX", "BITX", "stock"),  # 2x Bitcoin Strategy ETF
        ("BTCL", "BTCL", "stock"),  # T-Rex 2X Long Bitcoin Daily Target ETF
        ("BTCZ", "BTCZ", "stock"),  # T-Rex 2X Inverse Bitcoin Daily Target ETF
        ("SBIT", "SBIT", "stock"),  # ProShares UltraShort Bitcoin ETF
        ("COIN", "COIN", "stock"),
        ("COIG", "COIG", "stock"),  # Leverage Shares 2X Long COIN Daily ETF
        ("COIO", "COIO", "stock"),  # Leverage Shares 2x Capped Accelerated COIN Monthly ETF
        ("CONI", "CONI", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Short COIN Daily ET
        ("CONL", "CONL", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Long COIN Daily ETF
        ("CONX", "CONX", "stock"),  # Direxion Daily COIN Bull 2X ETF
        ("MSTR", "MSTR", "stock"),
        ("MSDD", "MSDD", "stock"),  # GraniteShares 2x Short MSTR Daily ETF
        ("MSOO", "MSOO", "stock"),  # Leverage Shares 2x Capped Accelerated MSTR Monthly ETF
        ("MST", "MST", "stock"),  # Defiance Leveraged Long Income MSTR ETF
        ("MSTP", "MSTP", "stock"),  # GraniteShares 2x Long MSTR Daily ETF
        ("MSTU", "MSTU", "stock"),  # T-Rex 2X Long MSTR Daily Target ETF
        ("MSTX", "MSTX", "stock"),  # Tidal Trust II Defiance Daily Target 2x Long MSTR ETF
        ("MSTZ", "MSTZ", "stock"),  # T-Rex 2X Inverse MSTR Daily Target ETF
        ("SMST", "SMST", "stock"),  # Tidal Trust II Defiance Daily Target 2x Short MSTR ETF
        ("XYZ", "XYZ", "stock"),
        ("XYZG", "XYZG", "stock"),  # Leverage Shares 2X Long XYZ Daily ETF
    ]),
    ("Tech and Other", [
        ("ABNB", "ABNB", "stock"),
        ("ABNG", "ABNG", "stock"),  # Leverage Shares 2x Long ABNB Daily ETF
        ("BA", "BA", "stock"),
        ("BOED", "BOED", "stock"),  # Direxion Shares ETF Trust Direxion Daily BA Bear 1X ETF
        ("BOEG", "BOEG", "stock"),  # Leverage Shares 2X Long BA Daily ETF
        ("BOEU", "BOEU", "stock"),  # Direxion Shares ETF Trust Direxion Daily BA Bull 2X ETF
        ("CRM", "CRM", "stock"),
        ("CRMG", "CRMG", "stock"),  # Leverage Shares 2X Long CRM Daily ETF
        ("DIS", "DIS", "stock"),
        ("SPOT", "SPOT", "stock"),
        ("SPOG", "SPOG", "stock"),  # Leverage Shares 2X Long SPOT Daily ETF
        ("PYPL", "PYPL", "stock"),
        ("PYPG", "PYPG", "stock"),  # Leverage Shares 2X Long PYPL Daily ETF
        ("RACE", "RACE", "stock"),
        ("UBER", "UBER", "stock"),
        ("UBRL", "UBRL", "stock"),  # GraniteShares 2x Long UBER Daily ETF
    ]),
    ("Healthcare", [
        ("LLY", "LLY", "stock"),
        ("ELIL", "ELIL", "stock"),  # Direxion Shares ETF Trust Direxion Daily LLY Bull 2X ETF
        ("ELIS", "ELIS", "stock"),  # Direxion Shares ETF Trust Direxion Daily LLY Bear 1X ETF
        ("HIMS", "HIMS", "stock"),
        ("HIMZ", "HIMZ", "stock"),  # Defiance Daily Target 2X Long HIMS ETF
        ("MRNA", "MRNA", "stock"),
        ("UNH", "UNH", "stock"),
        ("UNHG", "UNHG", "stock"),  # Leverage Shares 2X Long UNH Daily ETF
    ]),
    ("Banks, Brokers, Finance", [
        ("BCS", "BCS", "stock"),
        ("BRK.B", "BRK.B", "stock"),
        ("BRKD", "BRKD", "stock"),  # Direxion Shares ETF Trust Direxion Daily BRKB Bear 1X ETF
        ("BRKU", "BRKU", "stock"),  # Direxion Shares ETF Trust Direxion Daily BRKB Bull 2X ETF
        ("FUTU", "FUTU", "stock"),
        ("FUTG", "FUTG", "stock"),  # Leverage Shares 2x Long FUTU Daily ETF
        ("GS", "GS", "stock"),
        ("HOOD", "HOOD", "stock"),
        ("HODU", "HODU", "stock"),  # Direxion Daily HOOD Bull 2X ETF
        ("HOOG", "HOOG", "stock"),  # Leverage Shares 2X Long HOOD Daily ETF
        ("HOOX", "HOOX", "stock"),  # Defiance Daily Target 2X Long HOOD ETF
        ("ROBN", "ROBN", "stock"),  # T-Rex 2X Long HOOD Daily Target ETF
        ("LYG", "LYG", "stock"),
        ("UBS", "UBS", "stock"),
        ("V", "V", "stock"),
    ]),
    ("EM and DMs", [
        ("UKX", "EWU", "stock"),
        ("CAC40", "EWQ", "stock"),
        ("DAX", "EWG", "stock"),
        ("IT40", "EWI", "stock"),
        ("VT", "VT", "stock"),
    ]),
    ("Bonds", [
        ("SHY", "SHY", "stock"),
        ("HYG", "HYG", "stock"),
        ("UJB", "UJB", "stock"),  # ProShares Ultra High Yield
    ]),
    ("Asia", [
        ("KWEB", "KWEB", "stock"),
        ("CWEB", "CWEB", "stock"),  # Direxion Daily CSI China Internet Index Bull 2X ETF
        ("EWT", "EWT", "stock"),
        ("EWJ", "EWJ", "stock"),
        ("EWV", "EWV", "stock"),  # ProShares Trust UltraShort MSCI Japan
        ("EZJ", "EZJ", "stock"),  # ProShares Ultra MSCI Japan
        ("EWY", "EWY", "stock"),
        ("KORU", "KORU", "stock"),  # Direxion Daily MSCI South Korea Bull 3X ETF
        ("BABA", "BABA", "stock"),
        ("BABU", "BABU", "stock"),  # Direxion Daily BABA Bull 2X ETF
        ("BABX", "BABX", "stock"),  # GraniteShares ETF Trust GraniteShares 2x Long BABA Daily ETF
        ("KBAB", "KBAB", "stock"),  # KraneShares 2x Long BABA Daily ETF
        ("JD", "JD", "stock"),
        ("KJD", "KJD", "stock"),  # KraneShares 2x Long JD Daily ETF
        ("BIDU", "BIDU", "stock"),
        ("BIDG", "BIDG", "stock"),  # Leverage Shares 2X Long BIDU Daily ETF
        ("KBDU", "KBDU", "stock"),  # KraneShares 2x Long BIDU Daily ETF
        ("NIO", "NIO", "stock"),
        ("NIOG", "NIOG", "stock"),  # Leverage Shares 2X Long NIO Daily ETF
    ]),
    ("EU Individuals", [
        ("AZN", "AZN", "stock"),
        ("DEO", "DEO", "stock"),
        ("VOD", "VOD", "stock"),
    ]),
    ("Commodities", [
        ("GOLD", "GLD", "stock"),
        ("SILVER", "SLV", "stock"),
        ("AGQ", "AGQ", "stock"),  # ProShares Ultra Silver
        ("ZSL", "ZSL", "stock"),  # ProShares UltraShort Silver
        ("GDX", "GDX", "stock"),
        ("DUST", "DUST", "stock"),  # Direxion Daily Gold Miners Index Bear 2X ETF
        ("JDST", "JDST", "stock"),  # Direxion Daily Junior Gold Miners Index Bear 2X ETF
        ("JNUG", "JNUG", "stock"),  # Direxion Daily Junior Gold Miners Index Bull 2X ETF
        ("NUGT", "NUGT", "stock"),  # Direxion Daily Gold Miners Index Bull 2X ETF
        ("COPPER", "CPER", "stock"),
        ("COPZ", "COPZ", "stock"),  # Defiance Daily Target 2X Long Copper ETF
        ("CPXR", "CPXR", "stock"),  # USCF Daily Target 2X Copper Index ETF
        ("PALL", "PALL", "stock"),
        ("CORN", "CORN", "stock"),
        ("CXRN", "CXRN", "stock"),  # Teucrium 2x Daily Corn ETF
        ("WEAT", "WEAT", "stock"),
        ("WXET", "WXET", "stock"),  # Teucrium 2x Daily Wheat ETF
        ("CANE", "CANE", "stock"),
    ]),
    ("Energy", [
        ("USO", "USO", "stock"),
        ("SCO", "SCO", "stock"),  # ProShares UltraShort Bloomberg Crude Oil
        ("UCO", "UCO", "stock"),  # ProShares Ultra Bloomberg Crude Oil
        ("UNG", "UNG", "stock"),
        ("BOIL", "BOIL", "stock"),  # ProShares Ultra Bloomberg Natural Gas
        ("KOLD", "KOLD", "stock"),  # ProShares UltraShort Bloomberg Natural Gas
        ("SHEL", "SHEL", "stock"),
        ("RIO", "RIO", "stock"),
        ("BP", "BP", "stock"),
        ("XOM", "XOM", "stock"),
        ("XOMX", "XOMX", "stock"),  # Direxion Shares ETF Trust Direxion Daily XOM Bull 2X ETF
        ("XOMZ", "XOMZ", "stock"),  # Direxion Shares ETF Trust Direxion Daily XOM Bear 1X ETF
    ]),
]
