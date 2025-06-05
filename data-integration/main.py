import os
import sys
import psycopg
import time
import json
import re
import logging
from datetime import datetime
from typing import List, Tuple, Optional

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

os.environ['POSTGRES_DB'] = 'postgres'
os.environ['POSTGRES_USER'] = 'postgres'
os.environ['POSTGRES_PASSWORD'] = input("Enter the password of the PostgreSQL base : ")
os.environ['POSTGRES_HOST'] = 'localhost'
os.environ['POSTGRES_PORT'] = '5432'

# Variables d'environnement
postgres_db = os.environ.get('POSTGRES_DB')
postgres_user = os.environ.get('POSTGRES_USER')
postgres_password = os.environ.get('POSTGRES_PASSWORD')
postgres_host = os.environ.get('POSTGRES_HOST')
postgres_port = os.environ.get('POSTGRES_PORT')
output_directory_sample = "data/output"
#output_directory_sample = r"D:\git_a_supr\SAE_6_01_VCOD_Cochet_Lebreton_Ouattara_Verly_Lagadec\data_collection\sample_output"
output_directory_scrapped = "data/output_added/json"

def get_connection_string() -> str:
    """Génère la chaîne de connexion PostgreSQL"""
    try:
        conn_str = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        logger.info("Chaîne de connexion générée avec succès")
        # Ne pas logger le mot de passe pour des raisons de sécurité
        safe_conn_str = f"postgresql://{postgres_user}:***@{postgres_host}:{postgres_port}/{postgres_db}"
        logger.debug(f"Connection string: {safe_conn_str}")
        return conn_str
    except Exception as e:
        logger.error(f"Erreur lors de la génération de la chaîne de connexion : {e}")
        raise

def execute_sql_script(path: str) -> None:
    """Exécute un script SQL depuis un fichier"""
    try:
        logger.info(f"Début d'exécution du script SQL : {path}")
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"Le fichier SQL {path} n'existe pas")
        
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                with open(path, 'r', encoding='utf-8') as f:
                    script_content = f.read()
                    cur.execute(script_content)
        
        logger.info(f"Script SQL {path} exécuté avec succès")
        
    except FileNotFoundError as e:
        logger.error(f"Fichier SQL non trouvé : {e}")
        raise
    except psycopg.Error as e:
        logger.error(f"Erreur PostgreSQL lors de l'exécution de {path} : {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue lors de l'exécution de {path} : {e}")
        raise

def clean_text(text: Optional[str]) -> Optional[str]:
    """Nettoie le texte en supprimant les caractères spéciaux"""
    if not isinstance(text, str):
        return text
    
    try:
        # Nettoie le texte
        text = text.replace('é', 'e').replace('è', 'e').replace('à', 'a')
        text = re.sub(r'é', 'e', text)
        text = re.sub(r'è', 'e', text)
        text = re.sub(r'à', 'a', text)
        text = re.sub(r'\\u00e9', 'e', text)  # Remplace \u00e9 par e
        text = re.sub(r'[^\x00-\x7F]+', '', text)  # Supprime les caractères non-ASCII
        return text
    except Exception as e:
        logger.warning(f"Erreur lors du nettoyage du texte '{text}' : {e}")
        return text

def load_json_files(directory: str) -> List[dict]:
    """Charge tous les fichiers JSON d'un répertoire"""
    try:
        logger.info(f"Chargement des fichiers JSON depuis : {directory}")
        
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Le répertoire {directory} n'existe pas")
        
        files = [f for f in os.listdir(directory) if f.endswith('.json')]
        logger.info(f"Trouvé {len(files)} fichiers JSON dans {directory}")
        
        json_data = []
        for file in files:
            try:
                file_path = os.path.join(directory, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Nettoyage des caractères Unicode
                    content = re.sub(r'\\u00e9', 'e', content)
                    data = json.loads(content)
                    json_data.append(data)
                    logger.debug(f"Fichier {file} chargé avec succès")
            except json.JSONDecodeError as e:
                logger.error(f"Erreur JSON dans le fichier {file} : {e}")
                continue
            except Exception as e:
                logger.error(f"Erreur lors du chargement de {file} : {e}")
                continue
        
        logger.info(f"Chargement terminé : {len(json_data)} fichiers traités avec succès")
        return json_data
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement des fichiers JSON : {e}")
        raise

def insert_wrk_tournaments() -> None:
    """Insère les données de tournois dans la table wrk_tournaments"""
    try:
        logger.info("Début d'insertion des données de tournois")
        
        tournaments = load_json_files(output_directory_sample)
        tournament_data = []
        
        for tournament in tournaments:
            try:
                tournament_data.append((
                    tournament['id'], 
                    clean_text(tournament['name']), 
                    datetime.strptime(tournament['date'], '%Y-%m-%dT%H:%M:%S.000Z'),
                    clean_text(tournament['organizer']), 
                    clean_text(tournament['format']), 
                    int(tournament['nb_players'])
                ))
            except KeyError as e:
                logger.warning(f"Clé manquante dans le tournoi {tournament.get('id', 'unknown')} : {e}")
                continue
            except ValueError as e:
                logger.warning(f"Erreur de conversion pour le tournoi {tournament.get('id', 'unknown')} : {e}")
                continue
        
        logger.info(f"Préparation de {len(tournament_data)} tournois pour insertion")
        
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO public.wrk_tournaments VALUES (%s, %s, %s, %s, %s, %s)", 
                    tournament_data
                )
        
        logger.info(f"Insertion réussie de {len(tournament_data)} tournois")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des tournois : {e}")
        raise

def anonymize_player_id(player_id: str, cursor) -> str:
    """Anonymise un ID de joueur en utilisant la fonction SQL"""
    try:
        cursor.execute("SELECT get_anonymous_player_id(%s)", (player_id,))
        result = cursor.fetchone()
        return result[0] if result else player_id
    except Exception as e:
        logger.warning(f"Erreur lors de l'anonymisation du joueur {player_id} : {e}")
        return player_id

def insert_wrk_decklists() -> None:
    """Insère les données de decklists dans la table wrk_decklists avec anonymisation"""
    try:
        logger.info("Début d'insertion des données de decklists avec anonymisation")
        
        tournaments = load_json_files(output_directory_sample)
        decklist_data = []
        total_cards = 0
        
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                for tournament in tournaments:
                    tournament_id = tournament.get('id')
                    players = tournament.get('players', [])
                    
                    for player in players:
                        original_player_id = player.get('id')
                        # Anonymisation du player_id
                        anonymous_player_id = anonymize_player_id(original_player_id, cur)
                        
                        decklist = player.get('decklist', [])
                        total_cards += len(decklist)
                        
                        for card in decklist:
                            try:
                                card_url = card.get('url', '')
                                url_parts = card_url.split('/cards/')
                                if len(url_parts) > 1:
                                    saison_booster = url_parts[-1].split('/')
                                    saison = saison_booster[0] if len(saison_booster) > 0 else ''
                                    booster = saison_booster[1] if len(saison_booster) > 1 else ''
                                else:
                                    saison = ''
                                    booster = ''
                                
                                decklist_data.append((
                                    tournament_id,
                                    anonymous_player_id,  # ID anonymisé
                                    clean_text(card.get('type', '')),
                                    clean_text(card.get('name', '')),
                                    card_url,
                                    saison,
                                    booster,
                                    int(card.get('count', 0))
                                ))
                            except (ValueError, KeyError) as e:
                                logger.warning(f"Erreur dans les données de carte (tournoi {tournament_id}, joueur {original_player_id}) : {e}")
                                continue
                
                logger.info(f"Traitement de {total_cards} cartes, {len(decklist_data)} valides pour insertion")
                
                cur.executemany(
                    "INSERT INTO public.wrk_decklists VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                    decklist_data
                )
        
        logger.info(f"Insertion réussie de {len(decklist_data)} entrées de decklist anonymisées")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des decklists : {e}")
        raise

def insert_wrk_matches() -> None:
    """Insère les données de matchs dans la table wrk_matches avec anonymisation"""
    try:
        logger.info("Début d'insertion des données de matchs avec anonymisation")
        
        tournaments = load_json_files(output_directory_sample)
        matches_data = []
        total_matches = 0
        
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                for tournament in tournaments:
                    tournament_id = tournament.get('id')
                    matches = tournament.get('matches', [])
                    total_matches += len(matches)
                    
                    for match in matches:
                        try:
                            # Récupération des résultats du match depuis 'match_results'
                            match_results = match.get('match_results', [])
                            
                            if len(match_results) < 2:
                                logger.warning(f"Match incomplet dans le tournoi {tournament_id}: moins de 2 joueurs")
                                continue
                            
                            # Récupération des données des deux joueurs
                            player1 = match_results[0]
                            player2 = match_results[1]
                            
                            # Anonymisation des IDs des joueurs
                            original_idp1 = player1['player_id']
                            original_idp2 = player2['player_id']
                            
                            idp1 = anonymize_player_id(original_idp1, cur)
                            idp2 = anonymize_player_id(original_idp2, cur)
                            
                            sc1 = player1['score']
                            sc2 = player2['score']
                            
                            # Détermination du gagnant (avec IDs anonymisés)
                            if sc1 > sc2:
                                victory_player = idp1
                                loser_player = idp2
                            elif sc2 > sc1: 
                                victory_player = idp2    
                                loser_player = idp1     
                            else:
                                victory_player = "No winner"
                                loser_player = "No loser"
                            
                            matches_data.append((
                                tournament_id,
                                idp1,
                                sc1,
                                idp2,
                                sc2,
                                victory_player,
                                loser_player
                            ))
                            
                        except (KeyError, IndexError) as e:
                            logger.warning(f"Erreur dans les données de match (tournoi {tournament_id}) : {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"Erreur inattendue dans le match (tournoi {tournament_id}) : {e}")
                            continue
                
                logger.info(f"Traitement de {total_matches} matchs, {len(matches_data)} valides pour insertion")
                
                cur.executemany(
                    "INSERT INTO public.wrk_matches VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                    matches_data
                )
        
        logger.info(f"Insertion réussie de {len(matches_data)} matchs anonymisés")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des matchs : {e}")
        raise

def get_anonymization_stats() -> None:
    """Affiche les statistiques d'anonymisation"""
    try:
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM public.wrk_player_mapping")
                total_players = cur.fetchone()[0]
                
                cur.execute("""
                    SELECT anonymous_player_id, original_player_id 
                    FROM public.wrk_player_mapping 
                    ORDER BY CAST(SUBSTRING(anonymous_player_id FROM 8) AS INTEGER)
                    LIMIT 10
                """)
                sample_mappings = cur.fetchall()
                
                logger.info(f"=== STATISTIQUES D'ANONYMISATION ===")
                logger.info(f"Nombre total de joueurs anonymisés : {total_players}")
                logger.info(f"Exemples de mapping (10 premiers) :")
                for anon_id, orig_id in sample_mappings:
                    logger.info(f"  {orig_id} -> {anon_id}")
                
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des statistiques : {e}")


def insert_wrk_infocard() -> None:
    """Insère les données d'information des cartes dans la table wrk_infocards"""
    try:
        logger.info("Début d'insertion des données d'information des cartes")
        
        cards_data = load_json_files(output_directory_scrapped)
        infocard_data = []
        total_cards = 0
        
        for card_file in cards_data:
            if isinstance(card_file, list):
                cards = card_file
            else:
                cards = [card_file]
            
            total_cards += len(cards)
            
            for card in cards:
                try:
                    infocard_data.append((
                        card.get('url', ''),
                        clean_text(card.get('nom', '')),
                        clean_text(card.get('type_carte', '')),
                        clean_text(card.get('sous_type', '')),
                        int(card['hp']) if card.get('hp') is not None and str(card.get('hp')).isdigit() else None,
                        clean_text(card.get('evolving_stage', '')),
                        clean_text(card.get('evolves_from', '')),
                        clean_text(card.get('competence_1_nom', '')),
                        card.get('competence_1_puissance'),
                        clean_text(card.get('competence_2_nom', '')),
                        card.get('competence_2_puissance'),
                        clean_text(card.get('faiblesse', '')),
                        int(card['retreat']) if card.get('retreat') is not None and str(card.get('retreat')).isdigit() else None
                    ))
                except (ValueError, KeyError) as e:
                    logger.warning(f"Erreur dans les données de carte {card.get('url', 'unknown')} : {e}")
                    continue
        
        logger.info(f"Traitement de {total_cards} cartes, {len(infocard_data)} valides pour insertion")
        
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO public.wrk_infocards VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                    infocard_data
                )
        
        logger.info(f"Insertion réussie de {len(infocard_data)} informations de cartes")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des informations de cartes : {e}")
        raise
        
def tournament_season():
    """Ajoute la saison la plus récente pour chaque tournoi dans la table tournaments"""
    try:
        logger.info("Début de l'ajout des saisons dans la table tournaments")
        
        season_history = ["P-A", "A1", "A1a", "A2", "A2a", "A2b", "A3", "A3a"]
        
        with psycopg.connect(get_connection_string()) as conn:
            with conn.cursor() as cur:
                # Étape 1 : Ajouter la colonne latest_season à la table tournaments si elle n'existe pas
                cur.execute("""
                    ALTER TABLE public.wrk_tournaments 
                    ADD COLUMN IF NOT EXISTS latest_season VARCHAR
                """)
                logger.info("Colonne latest_season ajoutée à wrk_tournaments")
                
                # Étape 2 : Récupérer tous les tournois depuis la table tournaments
                cur.execute("SELECT tournament_id FROM public.wrk_tournaments")
                tournaments = cur.fetchall()
                
                updated_count = 0
                
                # Étape 3 : Pour chaque tournoi, trouver la saison la plus récente
                for (tournament_id,) in tournaments:
                    # Récupérer toutes les saisons des cartes de ce tournoi
                    cur.execute("""
                        SELECT DISTINCT card_saison 
                        FROM public.wrk_decklists 
                        WHERE tournament_id = %s AND card_saison IS NOT NULL AND card_saison != ''
                    """, (tournament_id,))
                    
                    seasons_in_tournament = cur.fetchall()
                    
                    latest_season = None
                    latest_index = -1
                    
                    # Parcourir toutes les saisons de ce tournoi
                    for (card_season,) in seasons_in_tournament:
                        # Vérifier si cette saison est plus récente
                        if card_season in season_history:
                            season_index = season_history.index(card_season)
                            if season_index > latest_index:
                                latest_index = season_index
                                latest_season = card_season
                    
                    # Étape 4 : Mettre à jour la table tournaments avec la saison trouvée
                    if latest_season:
                        cur.execute("""
                            UPDATE public.wrk_tournaments 
                            SET latest_season = %s 
                            WHERE tournament_id = %s
                        """, (latest_season, tournament_id))
                        updated_count += 1
                        logger.debug(f"Tournoi {tournament_id}: saison mise à jour = {latest_season}")
                    else:
                        logger.debug(f"Tournoi {tournament_id}: aucune saison valide trouvée")
        
        logger.info(f"Mise à jour terminée : {updated_count} tournois mis à jour avec leur saison la plus récente")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout des saisons aux tournois : {e}")
        raise

def main():
    """Fonction principale d'exécution du pipeline ETL avec anonymisation"""
    try:
        logger.info("=== DÉBUT DU PIPELINE ETL POKEMON TCG POCKET AVEC ANONYMISATION ===")
        
        # Vérification des variables d'environnement
        required_env_vars = ['POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST', 'POSTGRES_PORT']
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
        
        if missing_vars:
            raise EnvironmentError(f"Variables d'environnement manquantes : {missing_vars}")
        
        logger.info("Variables d'environnement vérifiées")
        
        # Étape 1 : Création des tables de travail (avec table de mapping)
        logger.info("=== ÉTAPE 1 : Création des tables de travail ===")
        execute_sql_script("data-integration/00_create_wrk_tables.sql")
        
        # Étape 2 : Insertion des données de tournois
        logger.info("=== ÉTAPE 2 : Insertion des données de tournois ===")
        insert_wrk_tournaments()
        
        # Étape 3 : Insertion des données de decklists (avec anonymisation)
        logger.info("=== ÉTAPE 3 : Insertion des données de decklists (avec anonymisation) ===")
        insert_wrk_decklists()
        tournament_season()
        
        # Étape 4 : Insertion des données d'information des cartes
        logger.info("=== ÉTAPE 4 : Insertion des données d'information des cartes ===")
        insert_wrk_infocard()
        
        # Étape 5 : Insertion des données de matchs (avec anonymisation)
        logger.info("=== ÉTAPE 5 : Insertion des données de matchs (avec anonymisation) ===")
        insert_wrk_matches()
        
        # Étape 6 : Construction de la base de données des cartes
        logger.info("=== ÉTAPE 6 : Construction de la base de données des cartes ===")
        execute_sql_script("data-integration/01_dwh_cards.sql")
        
        logger.info("=== PIPELINE ETL TERMINÉ AVEC SUCCÈS ===")
        
    except Exception as e:
        logger.error(f"ERREUR CRITIQUE DANS LE PIPELINE ETL : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()