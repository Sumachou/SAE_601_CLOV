import logging
import json
import psycopg
import csv
import os
import sys

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

# Variables de connexion à PostgreSQL
postgres_db = 'postgres'
postgres_user = 'postgres'
postgres_password = input("Enter the password of the PostgreSQL base : ")
postgres_host = 'localhost'
postgres_port = '5432'

def get_connection_string():
    """Génère la chaîne de connexion PostgreSQL"""
    return f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

def export_all_tables_to_csv():
    """Exporte toutes les tables PostgreSQL en CSV"""
    
    # Tables à exclure
    excluded_tables = ['wrk_tournament_seasons', 'wrk_infocards', 'wrk_player_mapping', 'card_evolutions', 'wrk_players']
    
    # Créer un dossier pour les exports
    export_dir = "data/csv"
    os.makedirs(export_dir, exist_ok=True)
    
    with psycopg.connect(get_connection_string()) as conn:
        with conn.cursor() as cur:
            # Récupérer toutes les tables
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
            """)
            
            tables = cur.fetchall()
            
            for (table_name,) in tables:
                # Ignorer les tables exclues
                if table_name in excluded_tables:
                    print(f"skipping {table_name}")
                    continue
                    
                print(f"Exporting : {table_name}")
                
                # Exporter chaque table
                cur.execute(f"SELECT * FROM public.{table_name}")
                rows = cur.fetchall()
                
                # Récupérer les noms de colonnes
                col_names = [desc[0] for desc in cur.description]
                
                # Écrire en CSV
                csv_file = os.path.join(export_dir, f"{table_name}.csv")
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(col_names)  # En-têtes
                    writer.writerows(rows)      # Données
                
                print(f"{table_name}.csv has been created ({len(rows)} rows)")
                
                
export_all_tables_to_csv()