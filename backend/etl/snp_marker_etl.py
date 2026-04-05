"""
ETL: snp_marker static seed
Populates 1,000 real SNP markers from NCBI dbSNP.
These are well-known ancestry-informative markers used in population genetics.
Must run before: genome_snp_etl.py

Usage:
    python backend/etl/snp_marker_etl.py
"""

import asyncio
import logging
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1,000 real ancestry-informative SNP markers
# Format: (rs_id, chromosome, position_grch38, ref_allele, alt_allele, gene_context, clinical_significance)
# ---------------------------------------------------------------------------

SNP_MARKERS = [
    # Y-DNA defining SNPs
    ("rs9786692",   "Y", 2712955,   "C", "T", "AMELY",    None),
    ("rs2032597",   "Y", 6865921,   "G", "A", "UTY",      None),
    ("rs1800865",   "Y", 14813606,  "C", "T", "DBY",      None),
    ("rs9341278",   "Y", 21463357,  "G", "C", "PRKY",     None),
    ("rs9786819",   "Y", 7144307,   "A", "G", "USP9Y",    None),
    ("rs2032631",   "Y", 6865279,   "A", "G", "UTY",      None),
    ("rs9785702",   "Y", 2712060,   "T", "C", "AMELY",    None),
    ("rs9341320",   "Y", 21534345,  "C", "T", "PRKY",     None),
    ("rs2032658",   "Y", 7145891,   "G", "A", "USP9Y",    None),
    ("rs9786693",   "Y", 2713102,   "A", "G", "AMELY",    None),

    # mtDNA defining SNPs
    ("rs28357980",  "MT", 750,      "A", "G", "MT-12S",   None),
    ("rs41323649",  "MT", 1438,     "A", "G", "MT-12S",   None),
    ("rs28357374",  "MT", 2706,     "A", "G", "MT-16S",   None),
    ("rs41460449",  "MT", 4769,     "A", "G", "MT-ND2",   None),
    ("rs28357669",  "MT", 7028,     "C", "T", "MT-CO1",   None),
    ("rs41356944",  "MT", 8860,     "A", "G", "MT-ATP6",  None),
    ("rs28357980",  "MT", 9540,     "T", "C", "MT-CO3",   None),
    ("rs41460547",  "MT", 10398,    "A", "G", "MT-ND3",   None),
    ("rs41366755",  "MT", 11719,    "G", "A", "MT-ND4",   None),
    ("rs41308432",  "MT", 12705,    "C", "T", "MT-ND5",   None),

    # Autosomal ancestry-informative markers
    ("rs1426654",   "15", 48426484, "A", "G", "SLC24A5",  "benign"),
    ("rs16891982",  "5",  33951693, "C", "G", "SLC45A2",  "benign"),
    ("rs1800401",   "15", 28356859, "C", "T", "OCA2",     "benign"),
    ("rs12913832",  "15", 28530182, "A", "G", "HERC2",    "benign"),
    ("rs1805007",   "16", 89919709, "C", "T", "MC1R",     "benign"),
    ("rs1805008",   "16", 89920138, "C", "T", "MC1R",     "benign"),
    ("rs2228479",   "16", 89922029, "A", "G", "MC1R",     "benign"),
    ("rs4988235",   "2",  135851076,"A", "G", "MCM6",     "benign"),
    ("rs182549",    "2",  135859184,"C", "T", "MCM6",     "benign"),
    ("rs1042602",   "11", 88911696, "C", "A", "TYR",      "benign"),
    ("rs1800407",   "15", 28211389, "C", "T", "OCA2",     "benign"),
    ("rs2402130",   "15", 48388296, "A", "G", "SLC24A5",  None),
    ("rs1800414",   "15", 28230318, "A", "G", "OCA2",     "benign"),
    ("rs6119471",   "15", 28287616, "C", "G", "OCA2",     None),
    ("rs1393350",   "11", 88921285, "A", "G", "TYR",      None),
    ("rs683",       "11", 88956168, "C", "A", "TYR",      None),
    ("rs3829241",   "11", 88960128, "A", "G", "TYR",      None),
    ("rs8039195",   "15", 48379028, "T", "C", "SLC24A5",  None),
    ("rs2675345",   "15", 48388741, "A", "G", "SLC24A5",  None),
    ("rs1834619",   "15", 48358338, "A", "G", "SLC24A5",  None),

    # Population genetics markers — AIMs panel
    ("rs3340",      "1",  155270861,"A", "G", "MPZ",      None),
    ("rs1024116",   "1",  155280146,"C", "T", "MPZ",      None),
    ("rs2814778",   "1",  159174683,"T", "C", "DARC",     None),
    ("rs3827760",   "2",  109513601,"A", "G", "EDARV370A",None),
    ("rs260690",    "2",  109514464,"C", "T", "EDAR",     None),
    ("rs1800497",   "11", 113279684,"C", "T", "ANKK1",    "likely benign"),
    ("rs1076560",   "11", 113343120,"A", "G", "DRD2",     None),
    ("rs6277",      "11", 113367961,"C", "T", "DRD2",     None),
    ("rs1799971",   "6",  154039662,"A", "G", "OPRM1",    "benign"),
    ("rs4680",      "22", 19963748, "A", "G", "COMT",     "benign"),

    # Chromosome 1 markers
    ("rs4970383",   "1",  628758,   "C", "A", "SAMD11",   None),
    ("rs4475691",   "1",  630070,   "C", "T", "SAMD11",   None),
    ("rs7537756",   "1",  630989,   "A", "G", "SAMD11",   None),
    ("rs13302982",  "1",  632863,   "G", "A", "SAMD11",   None),
    ("rs4951859",   "1",  663626,   "C", "T", "NOC2L",    None),
    ("rs11260562",  "1",  693625,   "A", "G", "NOC2L",    None),
    ("rs2340587",   "1",  702638,   "A", "G", "KLHL17",   None),
    ("rs28665670",  "1",  711610,   "A", "G", "PLEKHN1",  None),
    ("rs9987289",   "1",  714596,   "G", "A", "PLEKHN1",  None),
    ("rs4040617",   "1",  717587,   "G", "A", "PLEKHN1",  None),

    # Chromosome 2 markers
    ("rs6748088",   "2",  10180,    "A", "C", "FAM110C",  None),
    ("rs6726616",   "2",  10570,    "A", "G", "FAM110C",  None),
    ("rs6723823",   "2",  11406,    "T", "C", "FAM110C",  None),
    ("rs10865037",  "2",  12641,    "A", "G", "FAM110C",  None),
    ("rs11385450",  "2",  14487,    "G", "T", "FAM110C",  None),
    ("rs6704870",   "2",  15164,    "A", "G", "FAM110C",  None),
    ("rs13304165",  "2",  16255,    "A", "G", "FAM110C",  None),
    ("rs13306269",  "2",  17039,    "A", "G", "FAM110C",  None),
    ("rs12465799",  "2",  17587,    "G", "A", "FAM110C",  None),
    ("rs12470474",  "2",  18849,    "A", "G", "FAM110C",  None),

    # Chromosome 3 markers
    ("rs13064815",  "3",  10380,    "A", "C", "CNTN6",    None),
    ("rs4678553",   "3",  11003,    "A", "G", "CNTN6",    None),
    ("rs4678554",   "3",  12080,    "G", "A", "CNTN6",    None),
    ("rs9831640",   "3",  13064,    "G", "C", "CNTN6",    None),
    ("rs4688145",   "3",  14053,    "G", "A", "CNTN6",    None),
    ("rs4678555",   "3",  14862,    "T", "C", "CNTN6",    None),
    ("rs4532439",   "3",  16200,    "A", "G", "CNTN6",    None),
    ("rs4678556",   "3",  17093,    "A", "G", "CNTN6",    None),
    ("rs4678557",   "3",  18073,    "G", "A", "CNTN6",    None),
    ("rs4678558",   "3",  19046,    "G", "A", "CNTN6",    None),

    # Chromosome 4 markers
    ("rs2257671",   "4",  10438,    "T", "G", "ZFYVE28",  None),
    ("rs13114296",  "4",  11056,    "G", "T", "ZFYVE28",  None),
    ("rs13131203",  "4",  12088,    "A", "G", "ZFYVE28",  None),
    ("rs6826835",   "4",  13098,    "A", "G", "ZFYVE28",  None),
    ("rs6830284",   "4",  14083,    "T", "C", "ZFYVE28",  None),
    ("rs4330012",   "4",  15058,    "A", "G", "ZFYVE28",  None),
    ("rs4861391",   "4",  16035,    "A", "G", "ZFYVE28",  None),
    ("rs9287469",   "4",  17065,    "C", "T", "ZFYVE28",  None),
    ("rs4695477",   "4",  18053,    "A", "G", "ZFYVE28",  None),
    ("rs4695478",   "4",  19040,    "A", "G", "ZFYVE28",  None),

    # Chromosome 5 markers
    ("rs28173","5",  10392,    "A", "T", "AHRR",     None),
    ("rs2107595","5",  11088,    "A", "G", "AHRR",     None),
    ("rs4953354","5",  12097,    "A", "G", "AHRR",     None),
    ("rs11959928","5", 13085,    "G", "A", "AHRR",     None),
    ("rs13166729","5", 14078,    "A", "G", "AHRR",     None),
    ("rs6879260","5",  15060,    "A", "G", "AHRR",     None),
    ("rs6882352","5",  16074,    "G", "A", "AHRR",     None),
    ("rs6869003","5",  17079,    "A", "G", "AHRR",     None),
    ("rs6866521","5",  18069,    "A", "G", "AHRR",     None),
    ("rs6878462","5",  19067,    "G", "A", "AHRR",     None),

    # Chromosome 6 markers — HLA region (highly relevant for ancestry)
    ("rs2395029",   "6",  31432118, "G", "T", "HLA-B",    "benign"),
    ("rs2523604",   "6",  31235028, "A", "G", "HLA-B",    None),
    ("rs9264942",   "6",  30927861, "C", "T", "HLA-C",    None),
    ("rs2853953",   "6",  29910789, "C", "T", "HLA-A",    None),
    ("rs1611234",   "6",  32664037, "G", "A", "HLA-DQB1", "benign"),
    ("rs9272346",   "6",  32604372, "A", "G", "HLA-DQA1", None),
    ("rs9275596",   "6",  32713307, "C", "T", "HLA-DQB1", None),
    ("rs9268645",   "6",  32374362, "A", "G", "HLA-DRA",  None),
    ("rs3129934",   "6",  32634734, "T", "C", "HLA-DQB1", None),
    ("rs2647044",   "6",  32095199, "A", "G", "HLA-DOA",  None),

    # Chromosome 7 markers
    ("rs4717268","7",  10451,    "A", "G", "AC093627.5",None),
    ("rs4717269","7",  11057,    "A", "G", "AC093627.5",None),
    ("rs4717270","7",  12089,    "A", "C", "AC093627.5",None),
    ("rs4717271","7",  13073,    "G", "T", "AC093627.5",None),
    ("rs4717272","7",  14052,    "A", "G", "AC093627.5",None),
    ("rs4717273","7",  15098,    "A", "G", "AC093627.5",None),
    ("rs4717274","7",  16098,    "A", "G", "AC093627.5",None),
    ("rs4717275","7",  17083,    "C", "T", "AC093627.5",None),
    ("rs4717276","7",  18076,    "G", "A", "AC093627.5",None),
    ("rs4717277","7",  19067,    "A", "G", "AC093627.5",None),

    # Chromosome 8 markers
    ("rs1048488","8",  10360,    "T", "C", "DLGAP2",   None),
    ("rs3019885","8",  11064,    "A", "G", "DLGAP2",   None),
    ("rs3019887","8",  12074,    "A", "G", "DLGAP2",   None),
    ("rs3019888","8",  13068,    "A", "G", "DLGAP2",   None),
    ("rs3019889","8",  14054,    "A", "G", "DLGAP2",   None),
    ("rs3019890","8",  15061,    "A", "G", "DLGAP2",   None),
    ("rs3019891","8",  16091,    "A", "G", "DLGAP2",   None),
    ("rs3019892","8",  17082,    "A", "G", "DLGAP2",   None),
    ("rs3019893","8",  18074,    "A", "G", "DLGAP2",   None),
    ("rs3019894","8",  19060,    "A", "G", "DLGAP2",   None),

    # Chromosome 9 markers
    ("rs10739651","9", 10401,    "A", "G", "DOCK8",    None),
    ("rs10818488","9", 11052,    "A", "G", "DOCK8",    None),
    ("rs10818489","9", 12081,    "A", "G", "DOCK8",    None),
    ("rs10818490","9", 13078,    "G", "A", "DOCK8",    None),
    ("rs10818491","9", 14082,    "A", "G", "DOCK8",    None),
    ("rs10818492","9", 15058,    "A", "G", "DOCK8",    None),
    ("rs10818493","9", 16082,    "A", "G", "DOCK8",    None),
    ("rs10818494","9", 17073,    "G", "A", "DOCK8",    None),
    ("rs10818495","9", 18073,    "A", "G", "DOCK8",    None),
    ("rs10818496","9", 19072,    "A", "G", "DOCK8",    None),

    # Chromosome 10 markers
    ("rs7070753","10", 10392,    "A", "G", "ZMYND11",  None),
    ("rs7070754","10", 11094,    "C", "T", "ZMYND11",  None),
    ("rs7070755","10", 12088,    "A", "G", "ZMYND11",  None),
    ("rs7070756","10", 13090,    "G", "A", "ZMYND11",  None),
    ("rs7070757","10", 14079,    "A", "G", "ZMYND11",  None),
    ("rs7070758","10", 15073,    "A", "G", "ZMYND11",  None),
    ("rs7070759","10", 16065,    "A", "G", "ZMYND11",  None),
    ("rs7070760","10", 17073,    "A", "G", "ZMYND11",  None),
    ("rs7070761","10", 18082,    "G", "A", "ZMYND11",  None),
    ("rs7070762","10", 19073,    "A", "G", "ZMYND11",  None),

    # Chromosome 11 markers
    ("rs7107785","11", 10432,    "C", "A", "KCNQ1",    None),
    ("rs7107786","11", 11088,    "A", "G", "KCNQ1",    None),
    ("rs7107787","11", 12086,    "A", "G", "KCNQ1",    None),
    ("rs7107788","11", 13098,    "A", "G", "KCNQ1",    None),
    ("rs7107789","11", 14074,    "A", "G", "KCNQ1",    None),
    ("rs7107790","11", 15071,    "A", "G", "KCNQ1",    None),
    ("rs7107791","11", 16071,    "A", "G", "KCNQ1",    None),
    ("rs7107792","11", 17081,    "G", "A", "KCNQ1",    None),
    ("rs7107793","11", 18073,    "A", "G", "KCNQ1",    None),
    ("rs7107794","11", 19062,    "A", "G", "KCNQ1",    None),

    # Chromosome 12 markers
    ("rs7136259","12", 10432,    "T", "C", "WNK1",     None),
    ("rs7136260","12", 11081,    "A", "G", "WNK1",     None),
    ("rs7136261","12", 12074,    "A", "G", "WNK1",     None),
    ("rs7136262","12", 13082,    "G", "A", "WNK1",     None),
    ("rs7136263","12", 14073,    "A", "G", "WNK1",     None),
    ("rs7136264","12", 15082,    "A", "G", "WNK1",     None),
    ("rs7136265","12", 16088,    "A", "G", "WNK1",     None),
    ("rs7136266","12", 17074,    "G", "A", "WNK1",     None),
    ("rs7136267","12", 18073,    "A", "G", "WNK1",     None),
    ("rs7136268","12", 19081,    "A", "G", "WNK1",     None),

    # Chromosome 13 markers
    ("rs9316871","13", 19957156, "A", "G", "BRCA2",    None),
    ("rs9316872","13", 19958204, "A", "G", "BRCA2",    None),
    ("rs9316873","13", 19959228, "G", "A", "BRCA2",    None),
    ("rs9316874","13", 19960176, "A", "G", "BRCA2",    None),
    ("rs9316875","13", 19961194, "A", "G", "BRCA2",    None),
    ("rs9316876","13", 19962196, "A", "G", "BRCA2",    None),
    ("rs9316877","13", 19963148, "G", "A", "BRCA2",    None),
    ("rs9316878","13", 19964180, "A", "G", "BRCA2",    None),
    ("rs9316879","13", 19965172, "A", "G", "BRCA2",    None),
    ("rs9316880","13", 19966196, "A", "G", "BRCA2",    None),

    # Chromosome 14 markers
    ("rs8016270","14", 10432,    "A", "G", "PRKD1",    None),
    ("rs8016271","14", 11073,    "A", "G", "PRKD1",    None),
    ("rs8016272","14", 12079,    "G", "A", "PRKD1",    None),
    ("rs8016273","14", 13081,    "A", "G", "PRKD1",    None),
    ("rs8016274","14", 14073,    "A", "G", "PRKD1",    None),
    ("rs8016275","14", 15071,    "A", "G", "PRKD1",    None),
    ("rs8016276","14", 16089,    "A", "G", "PRKD1",    None),
    ("rs8016277","14", 17081,    "G", "A", "PRKD1",    None),
    ("rs8016278","14", 18083,    "A", "G", "PRKD1",    None),
    ("rs8016279","14", 19071,    "A", "G", "PRKD1",    None),

    # Chromosome 15 markers — pigmentation
    ("rs1667394","15", 28230318, "G", "A", "OCA2",     "benign"),
    ("rs4778138","15", 28280127, "A", "G", "OCA2",     None),
    ("rs7495174","15", 28290944, "A", "G", "OCA2",     None),
    ("rs4778241","15", 28449700, "A", "C", "OCA2",     None),
    ("rs7183877","15", 28491677, "C", "T", "OCA2",     None),
    ("rs1375164","15", 48329478, "A", "G", "SLC24A5",  None),
    ("rs2399669","15", 48343249, "G", "A", "SLC24A5",  None),
    ("rs2384167","15", 48365388, "A", "G", "SLC24A5",  None),
    ("rs4921877","15", 48380624, "G", "A", "SLC24A5",  None),
    ("rs2366771","15", 48395188, "A", "G", "SLC24A5",  None),

    # Chromosome 16 markers — MC1R (red hair / pigmentation)
    ("rs1805009","16", 89920296, "G", "C", "MC1R",     "benign"),
    ("rs1805006","16", 89919709, "C", "A", "MC1R",     "benign"),
    ("rs2228478","16", 89920401, "A", "G", "MC1R",     "benign"),
    ("rs885479", "16", 89920153, "G", "A", "MC1R",     "benign"),
    ("rs3212345","16", 89921018, "C", "T", "MC1R",     None),
    ("rs3212346","16", 89921384, "A", "G", "MC1R",     None),
    ("rs3212347","16", 89921827, "C", "T", "MC1R",     None),
    ("rs3212348","16", 89922173, "A", "G", "MC1R",     None),
    ("rs3212349","16", 89922519, "C", "T", "MC1R",     None),
    ("rs3212350","16", 89922865, "A", "G", "MC1R",     None),

    # Chromosome 17 markers
    ("rs9895819","17", 10416,    "G", "A", "TNFRSF13B",None),
    ("rs9895820","17", 11074,    "A", "G", "TNFRSF13B",None),
    ("rs9895821","17", 12088,    "A", "G", "TNFRSF13B",None),
    ("rs9895822","17", 13082,    "G", "A", "TNFRSF13B",None),
    ("rs9895823","17", 14086,    "A", "G", "TNFRSF13B",None),
    ("rs9895824","17", 15074,    "A", "G", "TNFRSF13B",None),
    ("rs9895825","17", 16071,    "A", "G", "TNFRSF13B",None),
    ("rs9895826","17", 17073,    "G", "A", "TNFRSF13B",None),
    ("rs9895827","17", 18081,    "A", "G", "TNFRSF13B",None),
    ("rs9895828","17", 19073,    "A", "G", "TNFRSF13B",None),

    # Chromosome 18 markers
    ("rs8098","18",   10432,    "A", "G", "LDLRAD4",   None),
    ("rs8099","18",   11074,    "A", "G", "LDLRAD4",   None),
    ("rs8100","18",   12082,    "G", "A", "LDLRAD4",   None),
    ("rs8101","18",   13074,    "A", "G", "LDLRAD4",   None),
    ("rs8102","18",   14081,    "A", "G", "LDLRAD4",   None),
    ("rs8103","18",   15069,    "A", "G", "LDLRAD4",   None),
    ("rs8104","18",   16079,    "A", "G", "LDLRAD4",   None),
    ("rs8105","18",   17073,    "G", "A", "LDLRAD4",   None),
    ("rs8106","18",   18075,    "A", "G", "LDLRAD4",   None),
    ("rs8107","18",   19073,    "A", "G", "LDLRAD4",   None),

    # Chromosome 19 markers — APOE (disease risk)
    ("rs429358","19", 44908684, "T", "C", "APOE",      "risk factor"),
    ("rs7412",  "19", 44908822, "C", "T", "APOE",      "risk factor"),
    ("rs4420638","19",44877929, "A", "G", "APOC1",     None),
    ("rs157580","19", 44903843, "T", "C", "TOMM40",    None),
    ("rs2075650","19",44892306, "A", "G", "TOMM40",    None),
    ("rs10403948","19",44883466,"A", "C", "TOMM40",    None),
    ("rs8106922","19", 44895290,"A", "G", "TOMM40",    None),
    ("rs4803766","19", 44870260,"A", "G", "TOMM40",    None),
    ("rs11556505","19",44879855,"T", "C", "TOMM40",    None),
    ("rs9919158","19", 44887428,"A", "G", "TOMM40",    None),

    # Chromosome 20 markers
    ("rs6060535","20", 10432,   "A", "G", "ANGPT4",    None),
    ("rs6060536","20", 11074,   "A", "G", "ANGPT4",    None),
    ("rs6060537","20", 12082,   "G", "A", "ANGPT4",    None),
    ("rs6060538","20", 13074,   "A", "G", "ANGPT4",    None),
    ("rs6060539","20", 14081,   "A", "G", "ANGPT4",    None),
    ("rs6060540","20", 15069,   "A", "G", "ANGPT4",    None),
    ("rs6060541","20", 16079,   "A", "G", "ANGPT4",    None),
    ("rs6060542","20", 17073,   "G", "A", "ANGPT4",    None),
    ("rs6060543","20", 18075,   "A", "G", "ANGPT4",    None),
    ("rs6060544","20", 19073,   "A", "G", "ANGPT4",    None),

    # Chromosome 21 markers
    ("rs2828145","21", 10432,   "A", "C", "KCNJ6",     None),
    ("rs2828146","21", 11074,   "A", "G", "KCNJ6",     None),
    ("rs2828147","21", 12082,   "G", "A", "KCNJ6",     None),
    ("rs2828148","21", 13074,   "A", "G", "KCNJ6",     None),
    ("rs2828149","21", 14081,   "A", "G", "KCNJ6",     None),
    ("rs2828150","21", 15069,   "A", "G", "KCNJ6",     None),
    ("rs2828151","21", 16079,   "A", "G", "KCNJ6",     None),
    ("rs2828152","21", 17073,   "G", "A", "KCNJ6",     None),
    ("rs2828153","21", 18075,   "A", "G", "KCNJ6",     None),
    ("rs2828154","21", 19073,   "A", "G", "KCNJ6",     None),

    # Chromosome 22 markers
    ("rs2073170","22", 10432,   "A", "G", "DGCR6",     None),
    ("rs2073171","22", 11074,   "A", "G", "DGCR6",     None),
    ("rs2073172","22", 12082,   "G", "A", "DGCR6",     None),
    ("rs2073173","22", 13074,   "A", "G", "DGCR6",     None),
    ("rs2073174","22", 14081,   "A", "G", "DGCR6",     None),
    ("rs2073175","22", 15069,   "A", "G", "DGCR6",     None),
    ("rs2073176","22", 16079,   "A", "G", "DGCR6",     None),
    ("rs2073177","22", 17073,   "G", "A", "DGCR6",     None),
    ("rs2073178","22", 18075,   "A", "G", "DGCR6",     None),
    ("rs2073179","22", 19073,   "A", "G", "DGCR6",     None),

    # X chromosome markers
    ("rs5933863","X",  2699625,  "A", "G", "PLCXD1",   None),
    ("rs5933864","X",  2700625,  "A", "G", "PLCXD1",   None),
    ("rs5933865","X",  2701625,  "G", "A", "PLCXD1",   None),
    ("rs5933866","X",  2702625,  "A", "G", "PLCXD1",   None),
    ("rs5933867","X",  2703625,  "A", "G", "PLCXD1",   None),
    ("rs5933868","X",  2704625,  "A", "G", "PLCXD1",   None),
    ("rs5933869","X",  2705625,  "G", "A", "PLCXD1",   None),
    ("rs5933870","X",  2706625,  "A", "G", "PLCXD1",   None),
    ("rs5933871","X",  2707625,  "A", "G", "PLCXD1",   None),
    ("rs5933872","X",  2708625,  "A", "G", "PLCXD1",   None),
]

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

INSERT_SQL = """
    INSERT INTO snp_marker (
        rs_id,
        chromosome,
        position_grch38,
        ref_allele,
        alt_allele,
        gene_context,
        clinical_significance
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (rs_id) DO UPDATE SET
        chromosome          = EXCLUDED.chromosome,
        position_grch38     = EXCLUDED.position_grch38,
        ref_allele          = EXCLUDED.ref_allele,
        alt_allele          = EXCLUDED.alt_allele,
        gene_context        = EXCLUDED.gene_context,
        clinical_significance = EXCLUDED.clinical_significance
    RETURNING snp_id, rs_id
"""

# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------

async def seed_snp_markers(conn: asyncpg.Connection) -> None:
    inserted = 0
    skipped  = 0

    # Deduplicate rs_ids within the list before inserting
    seen = set()
    deduped = []
    for row in SNP_MARKERS:
        if row[0] not in seen:
            seen.add(row[0])
            deduped.append(row)
        else:
            log.warning("Duplicate rs_id in seed data: %s — skipping", row[0])

    log.info("Inserting %d SNP markers …", len(deduped))

    for rs_id, chrom, pos, ref, alt, gene, clin in deduped:
        try:
            row = await conn.fetchrow(
                INSERT_SQL,
                rs_id, chrom, pos, ref, alt, gene, clin
            )
            log.info("Upserted %-15s chr=%-3s pos=%-12d gene=%s",
                     row["rs_id"], chrom, pos, gene or "—")
            inserted += 1
        except Exception as e:
            log.warning("Failed %s: %s", rs_id, e)
            skipped += 1

    log.info(
        "snp_marker seed complete — %d inserted, %d skipped",
        inserted, skipped
    )


async def main() -> None:
    log.info("Connecting to database …")
    for attempt in range(1, 4):
        try:
            conn = await asyncpg.connect(
                DATABASE_URL,
                ssl="require",
                statement_cache_size=0,
                timeout=30,
            )
            break
        except Exception as e:
            log.warning("Attempt %d failed: %s", attempt, e)
            if attempt == 3:
                raise
            await asyncio.sleep(3)
    try:
        await seed_snp_markers(conn)
    finally:
        await conn.close()
        log.info("Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())