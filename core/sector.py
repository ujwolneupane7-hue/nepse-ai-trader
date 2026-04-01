"""
Sector Analysis and Detection
[OK] FIXED: Sector strength returns all sectors (Issue #6)
[OK] FIXED: Consistent sector mapping (Issue #10)
"""

import logging

logger = logging.getLogger(__name__)

sector_map = {
    # ── COMMERCIAL BANKS ──────────────────────────────────────────
    "ADBL": "BANKING", "CZBIL": "BANKING", "EBL": "BANKING", "GBIME": "BANKING",
    "HBL": "BANKING", "KBL": "BANKING", "LSL": "BANKING", "MBL": "BANKING",
    "NABIL": "BANKING", "NBL": "BANKING", "NICA": "BANKING", "NIMB": "BANKING",
    "NMB": "BANKING", "PCBL": "BANKING", "PRVU": "BANKING", "SANIMA": "BANKING",
    "SBI": "BANKING", "SCB": "BANKING", "SBL": "BANKING",

    # ── DEVELOPMENT BANKS ─────────────────────────────────────────
    "CORBL": "DEV_BANK", "EDBL": "DEV_BANK", "GBBL": "DEV_BANK", "GRDBL": "DEV_BANK",
    "JBBL": "DEV_BANK", "KRBL": "DEV_BANK", "MNBBL": "DEV_BANK", "MLBL": "DEV_BANK",
    "NABBC": "DEV_BANK", "SADBL": "DEV_BANK", "SAPDBL": "DEV_BANK", "SHINE": "DEV_BANK",
    "SINDU": "DEV_BANK",

    # ── FINANCE ───────────────────────────────────────────────────
    "BFC": "FINANCE", "CFCL": "FINANCE", "GMFIL": "FINANCE", "GUFL": "FINANCE",
    "ICFC": "FINANCE", "JFL": "FINANCE", "MFIL": "FINANCE", "NFS": "FINANCE",
    "PFL": "FINANCE", "PROFL": "FINANCE", "RLFL": "FINANCE", "SFCL": "FINANCE",
    "SIFC": "FINANCE",

    # ── MICROFINANCE ──────────────────────────────────────────────
    "ALBSL": "MICROFINANCE", "BPW": "MICROFINANCE", "CBBL": "MICROFINANCE",
    "CYC": "MICROFINANCE", "DDBL": "MICROFINANCE", "FMDL": "MICROFINANCE",
    "FOWAD": "MICROFINANCE", "GMFBS": "MICROFINANCE", "GVLBS": "MICROFINANCE",
    "ILBS": "MICROFINANCE", "JALPA": "MICROFINANCE", "KLBSL": "MICROFINANCE",
    "KMCDB": "MICROFINANCE", "LLBS": "MICROFINANCE", "MERO": "MICROFINANCE",
    "MLBS": "MICROFINANCE", "MLBSL": "MICROFINANCE", "NESDO": "MICROFINANCE",
    "NICLBSL": "MICROFINANCE", "NMBMF": "MICROFINANCE", "NUBL": "MICROFINANCE",
    "RSDC": "MICROFINANCE", "SABSL": "MICROFINANCE", "SDLBSL": "MICROFINANCE",
    "SKBBL": "MICROFINANCE", "SLBSL": "MICROFINANCE", "SMB": "MICROFINANCE",
    "SMFDB": "MICROFINANCE", "SWMF": "MICROFINANCE", "USLB": "MICROFINANCE",
    "VLBS": "MICROFINANCE", "VLUCL": "MICROFINANCE",

    # ── LIFE INSURANCE ────────────────────────────────────────────
    "ALICL": "LIFE_INS", "CLI": "LIFE_INS", "IMLI": "LIFE_INS", "LICN": "LIFE_INS",
    "NLIC": "LIFE_INS", "PLI": "LIFE_INS", "RNLI": "LIFE_INS", "SJLIC": "LIFE_INS",
    "SNLI": "LIFE_INS", "SRLI": "LIFE_INS",

    # ── NON-LIFE INSURANCE ────────────────────────────────────────
    "AIL": "NON_LIFE_INS", "EIC": "NON_LIFE_INS", "IGI": "NON_LIFE_INS",
    "KIL": "NON_LIFE_INS", "NICL": "NON_LIFE_INS", "NIL": "NON_LIFE_INS",
    "NLG": "NON_LIFE_INS", "PICL": "NON_LIFE_INS", "PRIN": "NON_LIFE_INS",
    "RBCL": "NON_LIFE_INS", "SALICO": "NON_LIFE_INS", "SGI": "NON_LIFE_INS",
    "SICL": "NON_LIFE_INS", "SPIL": "NON_LIFE_INS", "UAIL": "NON_LIFE_INS",

    # ── HYDROPOWER ────────────────────────────────────────────────
    "AHPC": "HYDRO", "AKJCL": "HYDRO", "AKPL": "HYDRO", "API": "HYDRO",
    "BARUN": "HYDRO", "BPCL": "HYDRO", "CHCL": "HYDRO", "CHL": "HYDRO",
    "DHPL": "HYDRO", "EHPL": "HYDRO", "GLH": "HYDRO", "HDHPC": "HYDRO",
    "HLB": "HYDRO", "HPPL": "HYDRO", "HURJA": "HYDRO", "JBHL": "HYDRO",
    "JOSHI": "HYDRO", "KKHC": "HYDRO", "KPCL": "HYDRO", "LEC": "HYDRO",
    "MBJC": "HYDRO", "MEHL": "HYDRO", "MEN": "HYDRO", "MHNL": "HYDRO",
    "MHPC": "HYDRO", "MKJC": "HYDRO", "NGPL": "HYDRO", "NHDL": "HYDRO",
    "NHPC": "HYDRO", "NYADI": "HYDRO", "PMHPL": "HYDRO", "PPCL": "HYDRO",
    "RADHI": "HYDRO", "RHPC": "HYDRO", "RHPL": "HYDRO", "RURU": "HYDRO",
    "SAHAS": "HYDRO", "SGHC": "HYDRO", "SHPC": "HYDRO", "SJCL": "HYDRO",
    "SMHL": "HYDRO", "SMJC": "HYDRO", "SPC": "HYDRO", "SPHL": "HYDRO",
    "SSHL": "HYDRO", "TPC": "HYDRO", "UHEWA": "HYDRO", "UMHL": "HYDRO",
    "UNHPL": "HYDRO", "UPCL": "HYDRO", "UPPER": "HYDRO", "VLB": "HYDRO",

    # ── HOTELS & TOURISM ──────────────────────────────────────────
    "CGH": "HOTEL", "CITY": "HOTEL", "KDL": "HOTEL", "OHL": "HOTEL",
    "SHL": "HOTEL", "TRH": "HOTEL",

    # ── MANUFACTURING & PROCESSING ────────────────────────────────
    "BNT": "MANUFACTURING", "GCIL": "MANUFACTURING", "HDL": "MANUFACTURING",
    "SARB": "MANUFACTURING", "SHIVM": "MANUFACTURING", "UNL": "MANUFACTURING",

    # ── INVESTMENT ────────────────────────────────────────────────
    "CHDC": "INVESTMENT", "CIT": "INVESTMENT", "ENL": "INVESTMENT",
    "HIDCL": "INVESTMENT", "NRIC": "INVESTMENT",

    # ── TRADING ───────────────────────────────────────────────────
    "BBC": "TRADING", "STC": "TRADING",

    # ── OTHERS ────────────────────────────────────────────────────
    "NRVN": "OTHERS", "NTC": "OTHERS"
}


def compute_sector_strength(df):
    """Calculate sector momentum"""
    try:
        sector_perf = {}

        for stock in df['Stock'].unique():
            if stock not in sector_map:
                continue

            sector = sector_map[stock]
            sub = df[df['Stock'] == stock]

            if len(sub) < 5:
                continue

            past_price = sub['Close'].iloc[-5]
            
            if past_price == 0 or past_price < 0:
                continue
                
            change = (sub['Close'].iloc[-1] - past_price) / past_price

            if sector not in sector_perf:
                sector_perf[sector] = []
            sector_perf[sector].append(change)

        # [OK] FIX: Initialize all sectors to 0.0
        final_strength = {}
        for sector_name in set(sector_map.values()):
            final_strength[sector_name] = 0.0
        
        # Then update with calculated values
        for s, v in sector_perf.items():
            if len(v) > 0:
                final_strength[s] = sum(v) / len(v)
        
        logger.debug(f"Sector strength: {final_strength}")
        return final_strength
    except Exception as e:
        logger.error(f"Error computing sector strength: {e}")
        return {}