"""
(LANCER POSTGRE)

LANCER 1ER SCRAP -> (création de ..data/output)
LANCER 2EME SCRAP -> (création de ..data/output_added)

LANCER LA TRANSFORMATION

LANCER PROG EXTRACT CSV -> (création de ..data/csv) 

SUPPRIMER (suppression de ..data/output & ..data/output_added)
"""

import os
import sys
import json
import shutil
import logging


# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('pokemon_etl.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

#scrapping
logger.info("starting tournaments scraping")
os.system("python scraping/scrap1.py")
logger.info("COMPLETED")

logger.info("starting cards scraping")
os.system("python scraping/scrap2cards.py")
logger.info("COMPLETED")

#data-integration
logger.info("starting PostgreSQL integration")
os.system("python data-integration/main.py")
logger.info("COMPLETED")

#data-extract
logger.info("starting CSV extraction")
os.system("python data-to-csv/csvextr.py")
logger.info("COMPLETED")


#data-clean
def supprimer_dossier(chemin):
    if os.path.exists(chemin):
        shutil.rmtree(chemin)
        logger.info(f"folder {chemin} deleted")
    else:
        logger.info(f"impossible to delete {chemin}")


supprimer_dossier("data/output")
supprimer_dossier("data/output_added")