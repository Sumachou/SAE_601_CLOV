import json
import os
import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin
import pandas as pd
import re

def extract_urls_from_json_files(directory_path):
    all_urls = set()
    
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            file_path = os.path.join(directory_path, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    
                    if 'players' in data:
                        for player in data['players']:
                            if 'decklist' in player:
                                for card in player['decklist']:
                                    if 'url' in card:
                                        all_urls.add(card['url'])
                                        
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"Erreur lors de la lecture du fichier {filename}: {e}")
    
    return sorted(list(all_urls))

def get_all_evolves_from_urls(soup, session):
    """
    Récupère TOUS les liens vers les cartes qui évoluent en ce Pokémon
    """
    type_elem = soup.find('p', class_='card-text-type')
    if not type_elem:
        return []
    
    if 'Evolves from' not in type_elem.get_text():
        return []
    
    evolves_link = type_elem.find('a')
    if not evolves_link:
        return []
    
    search_url = 'https://pocket.limitlesstcg.com' + evolves_link.get('href')
    
    try:
        response = session.get(search_url, timeout=10)
        response.raise_for_status()
        search_soup = BeautifulSoup(response.content, 'html.parser')
        
        # Trouver tous les liens vers les cartes dans la grille de recherche
        card_links = []
        
        # Chercher dans la grille de cartes
        card_grid = search_soup.find('div', class_='card-search-grid')
        if card_grid:
            # Trouver tous les liens vers les cartes (qui se terminent par un numéro, pas par des paramètres)
            links = card_grid.find_all('a', href=re.compile(r'/cards/[^?]+$'))
            for link in links:
                full_url = 'https://pocket.limitlesstcg.com' + link.get('href')
                card_links.append(full_url)
        
        # Si aucune carte trouvée dans la grille, essayer l'ancienne méthode
        if not card_links:
            card_link = search_soup.find('a', href=re.compile(r'/cards/[^?]+$'))
            if card_link:
                full_url = 'https://pocket.limitlesstcg.com' + card_link.get('href')
                card_links.append(full_url)
        
        print(f"    Trouvé {len(card_links)} cartes qui évoluent vers cette carte")
        return card_links
        
    except Exception as e:
        print(f"    Erreur lors de la récupération des cartes d'évolution: {e}")
        return []

def scrape_card_info(url, session):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        card_info = {
            'url': url,
            'nom': None,
            'type_carte': None,
            'sous_type': None,
            'hp': None,
            'evolving_stage': None,
            'evolves_from': [],  # Maintenant c'est une liste
            'competence_1_nom': None,
            'competence_1_puissance': None,
            'competence_2_nom': None,
            'competence_2_puissance': None,
            'faiblesse': None,
            'retreat': None
        }
        
        name_elem = soup.find('span', class_='card-text-name')
        if name_elem:
            link = name_elem.find('a')
            if link:
                card_info['nom'] = link.get_text().strip()
        
        type_elem = soup.find('p', class_='card-text-type')
        if type_elem:
            type_text = ' '.join(type_elem.get_text().split())
            
            if 'Trainer - ' in type_text:
                card_info['type_carte'] = 'Trainer'
                parts = type_text.split('Trainer - ')
                if len(parts) > 1:
                    card_info['sous_type'] = parts[1].strip()
                
            elif 'Pokémon - ' in type_text:
                card_info['type_carte'] = 'Pokémon'
                
                if 'Basic' in type_text:
                    card_info['evolving_stage'] = 'Basic'
                elif 'Stage 1' in type_text:
                    card_info['evolving_stage'] = 'Stage 1'
                elif 'Stage 2' in type_text:
                    card_info['evolving_stage'] = 'Stage 2'
                
                # Récupérer TOUS les liens d'évolution
                card_info['evolves_from'] = get_all_evolves_from_urls(soup, session)
        
        if card_info['type_carte'] == 'Pokémon':
            page_text = soup.get_text()
            
            hp_pattern = r'([A-Za-z]+)\s*-\s*(\d+)\s*HP'
            hp_match = re.search(hp_pattern, page_text)
            if hp_match:
                card_info['sous_type'] = hp_match.group(1).strip()
                card_info['hp'] = int(hp_match.group(2))
            
            attack_elements = soup.find_all('p', class_='card-text-attack-info')
            
            attacks = []
            for attack_elem in attack_elements:
                full_text = attack_elem.get_text().strip()
                
                symbol_elem = attack_elem.find('span', class_='ptcg-symbol')
                attack_text = full_text
                
                if symbol_elem:
                    symbol_text = symbol_elem.get_text().strip()
                    if attack_text.startswith(symbol_text):
                        attack_text = attack_text[len(symbol_text):].strip()
                
                # Pattern corrigé - le "-" doit être à la fin ou échappé
                match = re.match(r'^([A-Za-z\s\-\',]+?)(?:\s+(\d+[+x\-]?))?$', attack_text)
                
                if match:
                    attack_name = match.group(1).strip()
                    power_text = match.group(2)
                    
                    if power_text:
                        power = power_text  # Conserver comme string avec le "+", "x", "-" s'il existe
                    else:
                        power = "0"
                    
                    attacks.append((attack_name, power))
            
            if len(attacks) >= 1:
                card_info['competence_1_nom'] = attacks[0][0]
                card_info['competence_1_puissance'] = attacks[0][1]
            
            if len(attacks) >= 2:
                card_info['competence_2_nom'] = attacks[1][0]
                card_info['competence_2_puissance'] = attacks[1][1]
            
            weakness_pattern = r'Weakness:\s*([A-Za-z]+)'
            weakness_match = re.search(weakness_pattern, page_text)
            if weakness_match:
                card_info['faiblesse'] = weakness_match.group(1).strip()
            
            retreat_pattern = r'Retreat:\s*(\d+)'
            retreat_match = re.search(retreat_pattern, page_text)
            if retreat_match:
                card_info['retreat'] = int(retreat_match.group(1))
        
        return card_info
        
    except requests.RequestException as e:
        print(f"Erreur lors du scraping de {url}: {e}")
        return None
    except Exception as e:
        print(f"Erreur inattendue pour {url}: {e}")
        return None

def scrape_all_cards(urls, delay=1):
    cards_data = []
    failed_urls = []
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    total_urls = len(urls)
    
    for i, url in enumerate(urls, 1):
        print(f"Scraping {i}/{total_urls}: {url}")
        
        card_info = scrape_card_info(url, session)
        
        if card_info:
            cards_data.append(card_info)
            evolves_count = len(card_info['evolves_from']) if card_info['evolves_from'] else 0
            print(f"  Succès: {card_info['nom']} ({card_info['type_carte']}) - {evolves_count} évolutions trouvées")
        else:
            failed_urls.append(url)
            print(f"  Échec")
        
        if i < total_urls:
            time.sleep(delay)
    
    session.close()
    
    return cards_data, failed_urls

def save_results(cards_data, failed_urls, output_dir="data/output_added"):
    os.makedirs(output_dir, exist_ok=True)
    
    # Créer le dossier json s'il n'existe pas
    json_dir = os.path.join(output_dir, "json")
    os.makedirs(json_dir, exist_ok=True)
    
    json_path = os.path.join(json_dir, "cards_data.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(cards_data, f, indent=2, ensure_ascii=False)
    
    if cards_data:
        csv_data = []
        for card in cards_data:
            card_row = {
                'url': card['url'],
                'nom': card['nom'],
                'type_carte': card['type_carte'],
                'sous_type': card['sous_type'],
                'hp': int(card['hp']) if card['hp'] is not None else None,
                'evolving_stage': card['evolving_stage'],
                'evolves_from': '|'.join(card['evolves_from']) if card['evolves_from'] else '',  # Joindre les URLs avec |
                'evolves_from_count': len(card['evolves_from']) if card['evolves_from'] else 0,  # Ajouter le nombre
                'competence_1_nom': card['competence_1_nom'],
                'competence_1_puissance': card['competence_1_puissance'],
                'competence_2_nom': card['competence_2_nom'],
                'competence_2_puissance': card['competence_2_puissance'],
                'faiblesse': card['faiblesse'],
                'retreat': int(card['retreat']) if card['retreat'] is not None else None
            }
            csv_data.append(card_row)
        
        df = pd.DataFrame(csv_data)
        csv_path = os.path.join(output_dir, "cards_data.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8')
    
    if failed_urls:
        failed_path = os.path.join(output_dir, "failed_urls.txt")
        with open(failed_path, 'w', encoding='utf-8') as f:
            for url in failed_urls:
                f.write(f"{url}\n")
    
    print(f"\nRésultats sauvegardés dans: {output_dir}")
    print(f"   Cartes scrapées: {len(cards_data)}")
    print(f"   URLs échouées: {len(failed_urls)}")
    
    if cards_data:
        trainers = sum(1 for card in cards_data if card['type_carte'] == 'Trainer')
        pokemon = sum(1 for card in cards_data if card['type_carte'] == 'Pokémon')
        evolution_cards = sum(1 for card in cards_data if card['evolves_from'])
        
        print(f"   Cartes Trainer: {trainers}")
        print(f"   Cartes Pokémon: {pokemon}")
        print(f"   Cartes avec évolutions: {evolution_cards}")
        print(f"   Autres: {len(cards_data) - trainers - pokemon}")

def main():
    directory_path = r"data\output"
    
    print("Extraction des URLs depuis les fichiers JSON...")
    
    if not os.path.exists(directory_path):
        print(f"Le répertoire {directory_path} n'existe pas.")
        return
    
    urls = extract_urls_from_json_files(directory_path)
    
    if not urls:
        print("Aucune URL trouvée dans les fichiers JSON.")
        return
    
    print(f"{len(urls)} URLs uniques trouvées.")
    
    print("\nExemples d'URLs trouvées:")
    for url in urls[:5]:
        print(f"   - {url}")
    if len(urls) > 5:
        print(f"   ... et {len(urls) - 5} autres")
    
    response = input(f"\nCommencer le scraping de {len(urls)} cartes ? (y/N): ")
    if response.lower() != 'y':
        print("Scraping annulé.")
        return
    
    print("\nDébut du scraping...")
    cards_data, failed_urls = scrape_all_cards(urls, delay=1)
    
    print("\nSauvegarde des résultats...")
    save_results(cards_data, failed_urls)
    
    print("\nScraping terminé.")

if __name__ == "__main__":
    main()