# Adapted from code created by ChatGPT on January 5, 2024

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# Define the ranges or lists for Market_ID and their respective years
market_id_years = {
	# 1: [1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], # Prac_WTA
	# 2: [1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], # Prac_VS
	# 3: [1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], # Prac_Earnings 
	4: [1998], # Movie_ISK
	5: [1998], # Movie_EOS
	6: [1999], # Hessen_99
	7: [1999, 2000, 2001, 2002, 2003, 2004, 2005], # Comp_Ret
	8: [1999, 2000, 2001, 2002, 2003, 2004, 2005], # MSFT_Price
	# 9: 
	10: [1999], # Congress00
	11: [1999], # FranceEU99
	12: [1999], # Movie_MAT
	13: [1999], # Movie_TTI
	14: [1999], # GermanyEU99
	15: [1999], # EPparties99
	16: [1999, 2000], # RCONV00
	17: [1999, 2000], # DCONV00
	18: [1999, 2000], # NYSENATE
	19: [1999], # Saxony_99
	20: [1999, 2000, 2001], # FedPolicy
	21: [1999], # Movie_SH
	22: [1999], # Movie_WINE
	23: [2000], # TAIWAN_VS
	# 24: InactiveTrader
	25: [2000], # PRES00_VS
	26: [2000], # REFORM00
	27: [2000], # TAIWAN_W
	28: [2000], # Vors_CDU
	29: [2000], # PRES00_WTA
	30: [2000], # MX_VS
	31: [2000], # MX_Winner
	32: [2000, 2001], # GInflation
	33: [2000, 2001], # GRSpread
	34: [2000], # Movie_Grin
	35: [2000], # Movie_Six
	36: [2001], # Appointments
	37: [2000, 2001], # CAM_WTA
	# 38: 
	# 39: 
	# 40:
	41: [2001], # CAM_VS
	#42: [2001], # MA_dk_hit
	43: [2001], # FVparti
	# 44: [2001], # Prac_Call
	45: [2001], # MSFT-Price
	46: [2001], # MSFTPrice
	47: [2001], # FVdate
	48: [2001], # Comp-Ret
	49: [2001], # CompRet
	50: [2001], # FVprime
	51: [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], # FedPolicyB
	52: [2001], # NYCMAYOR
	53: [2001], # Movie_HPS
	54: [2001], # Movie_MINC
	55: [2001, 2002], # NLPres
	56: [2002], # NLPremier
	57: [2002], # NLTweedeKamer
	58: [2002], # NLCoalitie
	59: [2002], # Cong02
	60: [2002, 2003], # NLTK03
	61: [2002, 2003], # NLPvdA
	62: [2002], # Movie_Die
	63: [2002, 2003], # NLCoalitie03
	64: [2002, 2003], # NLPremier03
	65: [2003], # Movie_Sun
	66: [2003, 2004], # Pres04_VS
	67: [2003, 2004], # DConv04
	68: [2003], # RECALL_1
	69: [2003], # RECALL_2
	70: [2003], # Movie_Cat
	# 71: Prac_FLU
	72: [2004], # FLU_wk03_04
	73: [2004], # FLU_wk05_04
	74: [2004], # FLU_wk07_04
	75: [2004], # FLU_wk09_04
	76: [2004], # FLU_wk11_04
	77: [2004], # FLU_wk13_04
	78: [2004], # Pres04_WTA
	79: [2004], # Congress04
	80: [2004], # House04
	81: [2004], # Senate04
	82: [2004], # GOOGLE_LIN
	83: [2004], # GOOGLE_WTA             # IPO
	84: [2004], # FLU_wk40_04
	85: [2004], # FLU_wk42_04
	86: [2004], # FLU_wk44_04
	87: [2004], # FLU_wk46_04
	88: [2004], # FLU_wk48_04
	89: [2004], # FLU_wk50_04
	90: [2004], # Movie_SBob
	91: [2004, 2005], # FLU_wk52_04
	92: [2004, 2005], # FLU_wk02_05
	93: [2004, 2005], # FLU_wk04_05
	94: [2004, 2005], # FLU_wk06_05
	95: [2005], # FLU_wk08_05
	96: [2005], # FLU_wk10_05
	97: [2005], # FLU_wk12_05
	98: [2005], # FLU_wk14_05
	99: [2005], # FLU_wk16_05
	#100: Prac_HFM
	101: [2005], # HFM_Irene
	102: [2005], # HFM_Katrina
	103: [2005], # HFM_Kat_Gulf
	104: [2005], # HFM_Maria
	105: [2005], # HFM_Nate
	106: [2005], # HFM_Ophelia
	107: [2005], # HFM_Philippe
	108: [2005], # HFM_Rit
	109: [2005], # FLU_wk40_05
	110: [2005], # FLU_wk41_05
	111: [2005], # FLU_wk42_05
	112: [2005], # FLU_wk43_05
	113: [2005], # FLU_wk44_05
	114: [2005], # FLU_wk45_05
	115: [2005], # HFM_Stn
	116: [2005], # HFM_Tam
	117: [2005], # FLU_wk46_05
	118: [2005], # FLU_wk47_05
	119: [2005], # HFM_Wilma
	120: [2005], # FLU_wk48_05
	121: [2005], # HFM_Beta	
	122: [2005], # FLU_wk49_05	
	123: [2005], # FLU_wk50_05	
	124: [2005], # FLU_wk51_05
	125: [2005], # FLU_wk52_05
	126: [2005], # HFM_Gamma
	127: [2005, 2006], # FLU_wk01_06
	128: [2005, 2006], # FLU_wk02_06
	129: [2005, 2006], # FLU_wk03_06
	130: [2005, 2006], # FLU_wk04_06	
	131: [2005, 2006], # FLU_wk05_06	
	132: [2005, 2006], # FLU_wk06_06	
	133: [2006], # FLU_wk07_06	
	134: [2006], # FLU_wk08_06	
	135: [2006], # FLU_wk09_06	
	136: [2006], # FLU_wk10_06	
	137: [2006], # FLU_wk11_06	
	138: [2006], # FLU_wk12_06	
	139: [2006], # FLU_wk13_06	
	140: [2006], # FLU_wk14_06	
	141: [2006], # FLU_wk15_06	
	142: [2006], # FLU_wk16_06	
	143: [2006], # FLU_wk17_06	
	# 144 SuspendedAccnts
	145: [2006], # CONGRESS06
	146: [2006], # HOUSE06
	147: [2006], # SENATE06
	148: [2006, 2007, 2008], # PRES08_VS
	149: [2006, 2007, 2008], # PRES08_WTA
	150: [2006], # HFM_Ber	
	151: [2006], # HFM_Chr	
	152: [2006], # HFM_Deb	
	153: [2006], # HFM_Ern	
	154: [2006], # HFM_Flo	
	155: [2006], # HFM_Gor	
	156: [2006], # HFM_Hel	
	157: [2006], # HFM_Isa	
	158: [2006], # IAFLU_WK42_06	
	159: [2006], # IAFLU_WK43_06
	160: [2006], # IAFLU_WK44_06	
	161: [2006], # IAFLU_WK45_06	
	162: [2006], # IAFLU_WK46_06	
	163: [2006], # IAFLU_WK47_06	
	164: [2006], # IAFLU_WK48_06	
	165: [2006], # IAFLU_WK49_06	
	166: [2006], # NCFLU_WK44_06	
	167: [2006], # NCFLU_WK45_06	
	168: [2006], # NCFLU_WK46_06	
	169: [2006], # NCFLU_WK47_06
	170: [2006], # NCFLU_WK48_06
	171: [2006], # NCFLU_WK49_06
	172: [2006], # Movie_HFet
	173: [2006], # Movie_Font
	174: [2006], # IAFLU_WK50_06
	175: [2006], # NCFLU_WK50_06
	176: [2006], # IAFLU_WK51_06
	177: [2006], # NCFLU_WK51_06	
	178: [2006], # IAFLU_WK52_06
	179: [2006], # NCFLU_WK52_06
	180: [2006, 2007], # IAFLU_WK01_07	
	181: [2006, 2007], # NCFLU_WK01_07	
	182: [2006, 2007], # IAFLU_WK02_07	
	183: [2006, 2007], # NCFLU_WK02_07	
	184: [2006, 2007], # IAFLU_WK03_07	
	185: [2006, 2007], # NCFLU_WK03_07	
	186: [2006, 2007], # FVE_06-07	
	187: [2006, 2007], # IAFLU_WK04_07	
	188: [2006, 2007], # NCFLU_WK04_07	
	189: [2006, 2007], # IAFLU_WK05_07
	190: [2006, 2007], # NCFLU_WK05_07	
	191: [2006, 2007], # IAFLU_WK06_07	
	192: [2006, 2007], # NCFLU_WK06_07	
	193: [2007], # IAFLU_WK07_07	
	194: [2007], # NCFLU_WK07_07	
	195: [2007], # IAFLU_WK08_07	
	196: [2007], # NCFLU_WK08_07	
	197: [2007, 2008, 2009], # AFPolicy
	198: [2007], # IAFLU_WK09_07	
	199: [2007], # NCFLU_WK09_07
	200: [2007], # IAFLU_WK10_07	
	201: [2007], # NCFLU_WK10_07	
	202: [2007], # IAFLU_WK11_07	
	203: [2007], # NCFLU_WK11_07	
	204: [2007], # IAFLU_WK12_07	
	205: [2007], # NCFLU_WK12_07	
	206: [2007], # AFHumNum	
	207: [2007], # AFHumLoc	
	208: [2007], # AFAnimal	
	209: [2007], # IAFLU_WK13_07
	210: [2007], # NCFLU_WK13_07	
	211: [2007], # Movie_THRE	
	212: [2007], # IAFLU_WK14_07	
	213: [2007], # NCFLU_WK14_07	
	214: [2007], # DConv08	
	215: [2007], # RConv08 	
	216: [2007], # IAFLU_WK15_07	
	217: [2007], # NCFLU_WK15_07	
	218: [2007], # IAFLU_WK16_07	
	219: [2007], # NCFLU_WK16_07
	220: [2007], # IAFLU_WK17_07
	221: [2007], # NCFLU_WK17_07
	222: [2007], # IAFLU_WK18_07
	223: [2007], # NCFLU_WK18_07
	224: [2007], # PDPRIM_VS
	225: [2007], # PDPRIM_WTA
	226: [2007], # HFM_DEAN
	227: [2007], # HFM_ERI
	228: [2007], # HFM_FEL
	229: [2007], # HFM_GAB
	230: [2007], # HFM_HUM
	231: [2007], # HFM_ING
	232: [2007], # IAFLU_WK40
	233: [2007], # IAFLU_WK41
	234: [2007], # IAFLU_WK42
	235: [2007], # IAFLU_WK43
	236: [2007], # NCFLU_WK40
	237: [2007], # NCFLU_WK41
	238: [2007], # NCFLU_WK42
	239: [2007], # NCFLU_WK43
	240: [2007], # IAFLU_WK44
	241: [2007], # NCFLU_WK44
	242: [2007], # HFM_KAR
	243: [2007], # IAFLU_WK45
	244: [2007], # NCFLU_WK45
	245: [2007], # NEFLU_WK40
	246: [2007], # NEFLU_WK41
	247: [2007], # IAFLU_WK46
	248: [2007], # NCFLU_WK46
	249: [2007], # IAFLU_WK47
	250: [2007], # NCFLU_WK47
	251: [2007], # NEFLU_WK42
	252: [2007], # NEFLU_WK43
	253: [2007], # NEFLU_WK44
	254: [2007], # NEFLU_WK45
	255: [2007], # NEFLU_WK46
	256: [2007], # NEFLU_WK47
	257: [2007], # IAFLU_WK48
	258: [2007], # NCFLU_WK48
	259: [2007], # NEFLU_WK48
	260: [2007], # IAFLU_WK49
	261: [2007], # NCFLU_WK49
	262: [2007], # NEFLU_WK49
	263: [2007, 2008], # HFM_NOE	
	264: [2007], # Movie_BEOW	
	265: [2007], # IAFLU_WK50
	266: [2007], # NCFLU_WK50
	267: [2007], # NEFLU_WK50
	268: [2007], # IAFLU_WK51
	269: [2007], # NCFLU_WK51
	270: [2007], # NEFLU_WK51
	271: [2007], # IAFLU_WK52
	272: [2007, 2008], # IAFLU_WK01
	273: [2007], # NCFLU_WK52
	274: [2007, 2008], # NCFLU_WK01
	275: [2007], # NEFLU_WK52
	276: [2007, 2008], # NEFLU_WK01
	277: [2007, 2008], # IAFLU_WK02
	278: [2007, 2008], # NCFLU_WK02
	279: [2007, 2008], # NEFLU_WK02
	280: [2007, 2008], # IAFLU_WK03
	281: [2007, 2008], # NCFLU_WK03
	282: [2007, 2008], # NEFLU_WK03
	283: [2007, 2008], # IAFLU_WK04
	284: [2007, 2008], # NCFLU_WK04
	285: [2007, 2008], # NEFLU_WK04
	286: [2007, 2008], # FVE_07-08
	287: [2007, 2008], # IAFLU_WK05
	288: [2007, 2008], # NCFLU_WK05
	289: [2007, 2008], # NEFLU_WK05
	290: [2007, 2008], # IAFLU_WK06
	291: [2007, 2008], # NCFLU_WK06
	292: [2007, 2008], # NEFLU_WK06
	293: [2008], # IAFLU_WK07
	294: [2008], # NCFLU_WK07
	295: [2008], # NEFLU_WK07
	296: [2008], # IAFLU_WK08
	297: [2008], # NCFLU_WK08
	298: [2008], # NEFLU_WK08
	299: [2008], # IAFLU_WK09
	300: [2008], # NCFLU_WK09
	301: [2008], # NEFLU_WK09
	302: [2008], # IAFLU_WK10
	303: [2008], # NCFLU_WK10
	304: [2008], # NEFLU_WK10
	305: [2008], # IAFLU_WK11
	306: [2008], # NCFLU_WK11
	307: [2008], # NEFLU_WK11
	308: [2008], # IAFLU_WK12
	309: [2008], # NCFLU_WK12
	310: [2008], # NEFLU_WK12
	311: [2008], # IAFLU_WK13
	312: [2008], # NCFLU_WK13
	313: [2008], # NEFLU_WK13
	314: [2008], # IAFLU_WK14
	315: [2008], # NCFLU_WK14
	316: [2008], # NEFLU_WK14
	317: [2008], # IAFLU_WK15
	318: [2008], # NCFLU_WK15
	319: [2008], # NEFLU_WK15
	320: [2008], # IAFLU_WK16
	321: [2008], # NCFLU_WK16
	322: [2008], # NEFLU_WK16
	323: [2008], # IAFLU_WK17
	324: [2008], # NCFLU_WK17
	325: [2008], # NEFLU_WK17
	326: [2008], # SWmrsa2008
	327: [2008], # IAFLU_WK18
	328: [2008], # NCFLU_WK18
	329: [2008], # NEFLU_WK18
	330: [2008], # IAFLU_WK19
	331: [2008], # NCFLU_WK19
	332: [2008], # NEFLU_WK19
	333: [2008], # IAFLU_WK20
	334: [2008], # NCFLU_WK20
	335: [2008], # NEFLU_WK20
	336: [2008], # MNSen08_VS
	337: [2008], # MNSen08_WTA
	338: [2008], # Congress08
	339: [2008], # House08
	340: [2008], # Senate08
	341: [2008], # Movie_Twlt
	342: [2009, 2010], # UN_INT
	343: [2009, 2010], # Congress10
	344: [2009, 2010], # House10
	345: [2009, 2010], # Senate10
	346: [2009, 2010], # CPI_WTA	
	347: [2010], # FLSen10_VS
	348: [2010], # FLSen10_WTA
	349: [2011], # Movie_AdjB
	350: [2011, 2012], # PRES12_VS
	351: [2011, 2012], # PRES12_WTA
	352: [2011, 2012], # Congress12
	353: [2011, 2012], # House12
	354: [2011, 2012], # Senate12
	355: [2011, 2012], # IACaucus12
	356: [2011, 2012], # RCONV12
	357: [2012, 2013, 2014], # Congress14
	358: [2012, 2013, 2014], # House14
	359: [2012, 2013, 2014], # Senate14
	360: [2014, 2015, 2016], # Congress16
	361: [2014, 2015, 2016], # PRES16_VS
	362: [2014, 2015, 2016], # Pres16_WTA
	363: [2015], # Movie_50Sh
	364: [2016], # RCONV16
	365: [2016], # DCONV16
	# 366 TAs1_3upgl
	367: [2016, 2017, 2018], # Senate18
	368: [2016, 2017, 2018], # House18
	369: [2016, 2017, 2018], # Congress18
	# 370 TAs1_9pkc7
	# 371 TAs1_m0nhe
	# 372 TAs1_h36tm
	# 373 TAs2_vbq3o
	# 374 TAs1_8negg
	375: [2017], # Movie_Coco
	# 376 TAs1_ijb6e
	377: [2017, 2018], # MSFT_PL
	# 378 TAs1_e65ol
	# 379 TAs1_tlnu4
	# 380 TAs1_91vp6
	# 381 TAs1_ofdvc
	# 382 
	383: [2019, 2020], # PRES20_VS
	384: [2019, 2020], # PRES20_WTA
	385: [2019, 2020], # CONGRESS20
	386: [2019, 2020], # IntInd
	# 387
	388: [2020], # Index
	389: [2019, 2020], # SENATE20
	390: [2019, 2020], # HOUSE20
	391: [2020], # DCONV20
	# 392 TestMar_c48eh
	# 393 TestMar_vo2dy
	# 394 TestMar_kv5x4
	# 395 TestMar_cbngh
	# 396 TestMar_66rqy
	# 397 TestMar_yyazc
	# 398 TestMar_oogdy
	# 399 TestMar_5v56e
	# 400 TestMar_i12hh
	# 401 TestMar_2qia6
	# 402 TestMar_eaou8
	# 403 TestMar_ifsjd
	# 404 TestMar_e9fbb
	# 405 TestMar_gqyro
	# 406 TestMar_bvg6j
	407: [2021, 2022], # CONGRESS22
	408: [2021, 2022], # HOUSE22
	409: [2021, 2022], # SENATE22
	410: [2021, 2022], # FR22R1_WTA
	411: [2021, 2022], # FR22R2_WTA
	412: [2021, 2022], # FR22_CAND
	413: [2022], # QUEB22_SS
	414: [2022], # CHAV22_WTA
	415: [2023, 2024], # House24
	416: [2023, 2024], # Senate24
    417: [2023, 2024], # Congress24
	418: [2023, 2024], # DCONV24 
	419: [2023, 2024], # RCONV24
	420: [2023, 2024], # Pres24_VS
	421: [2023, 2024] # PRES24_WTA
	# 422 TestMar_9srqt
	# 423
}
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

# List to hold all dataframes
all_data = []

# Custom function to process each row
def process_row(row_html):
    # Fix missing </td> tags
    fixed_html = re.sub(r'(<td[^>]*>[^<]+)(<td)', r'\1</td>\2', row_html)

    # Extract cell contents
    cells = re.findall(r'<td.*?>(.*?)</td>', fixed_html, re.DOTALL)
    processed_cells = [re.sub('<[^<]+?>', '', cell).strip() for cell in cells]
    return processed_cells
	
# Loop through each Market_ID and its respective years
for market_id, years in market_id_years.items():
    for year in years:
        for month in months:
            url = f"https://iemweb.biz.uiowa.edu/iem_pricehistory/pricehistory/?Market_ID={market_id}&Month={month}&Year={year}"
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract the page title
                page_title_tag = soup.find('h2', {'class': 'page-title bold-headline--serif bold-headline'})
                page_title = page_title_tag.get_text(strip=True) if page_title_tag else 'No Title'

                table = soup.find('table', {'class': 'table table--is-striped table--static table--width-default'})

                if table:
                    headers = [th.text.strip() for th in table.find_all('th')]
                    rows_html = [str(tr) for tr in table.find_all('tr') if tr.find_all('td')]
                    rows = [process_row(row_html) for row_html in rows_html]

                    if rows:
                        df = pd.DataFrame(rows, columns=headers)
                        df['Market_ID'] = market_id
                        df['Month'] = month
                        df['Year'] = year
                        df['Page_Title'] = page_title.replace("Price History for  ", "")  # Strip the prefix
                        all_data.append(df)
                    else:
                        print(f"No data rows found for Market ID: {market_id}, Month: {month}, Year: {year}")
                else:
                    print(f"No table found for Market ID: {market_id}, Month: {month}, Year: {year}")
            else:
                print(f"Failed to retrieve content from {url}, status code: {response.status_code}")

# Concatenate all dataframes
final_df = pd.concat(all_data, ignore_index=True)

# Write the final dataframe to a CSV file
output_file = 'IEM_market_data.csv'
final_df.to_csv(output_file, index=False)
print(f"Data written to {output_file}")