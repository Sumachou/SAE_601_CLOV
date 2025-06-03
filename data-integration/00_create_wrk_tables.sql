-- Table de mapping pour l'anonymisation des joueurs
DROP TABLE IF EXISTS public.wrk_player_mapping;
CREATE TABLE public.wrk_player_mapping (
  original_player_id varchar PRIMARY KEY,
  anonymous_player_id varchar NOT NULL
);

-- public.tournaments definition
DROP TABLE IF EXISTS public.wrk_tournaments;
CREATE TABLE public.wrk_tournaments (
  tournament_id varchar NULL,
  tournament_name varchar NULL,
  tournament_date timestamp NULL,
  tournament_organizer varchar NULL,
  tournament_format varchar NULL,
  tournament_nb_players int NULL
);

DROP TABLE IF EXISTS public.wrk_decklists;
CREATE TABLE public.wrk_decklists (
  tournament_id varchar NULL,
  player_id varchar NULL,
  card_type varchar NULL,
  card_name varchar NULL,
  card_url varchar NULL,
  card_saison varchar NULL,
  card_booster varchar NULL,
  card_count int NULL
);

DROP TABLE IF EXISTS public.wrk_infocards;
CREATE TABLE public.wrk_infocards (
  url varchar NULL,
  nom varchar NULL,
  type_carte varchar NULL,
  sous_type varchar NULL,
  hp int NULL,
  evolving_stage varchar NULL,
  evolves_from varchar NULL,
  competence_1_nom varchar NULL,
  competence_1_puissance varchar NULL,
  competence_2_nom varchar NULL,
  competence_2_puissance varchar NULL,
  faiblesse varchar NULL,
  retreat int NULL
);

DROP TABLE IF EXISTS public.wrk_matches;
CREATE TABLE public.wrk_matches (
  tournament_id varchar NULL,
  idp1 varchar NULL,
  sc1 varchar NULL,
  idp2 varchar NULL,
  sc2 varchar NULL,
  victory_player varchar NULL,
  loser_player varchar NULL
);

-- Fonction pour obtenir ou créer un ID anonymisé
CREATE OR REPLACE FUNCTION get_anonymous_player_id(original_id varchar) 
RETURNS varchar AS $$
DECLARE
    anonymous_id varchar;
    next_id integer;
BEGIN
    -- Chercher si le joueur existe déjà dans la table de mapping
    SELECT anonymous_player_id INTO anonymous_id 
    FROM public.wrk_player_mapping 
    WHERE original_player_id = original_id;
    
    -- Si le joueur n'existe pas, on crée un nouvel ID
    IF anonymous_id IS NULL THEN
        -- Obtenir le prochain numéro séquentiel
        SELECT COALESCE(MAX(CAST(SUBSTRING(anonymous_player_id FROM 8) AS INTEGER)), 0) + 1 
        INTO next_id 
        FROM public.wrk_player_mapping 
        WHERE anonymous_player_id LIKE 'PLAYER_%';
        
        -- Créer le nouvel ID anonyme
        anonymous_id := 'PLAYER_' || LPAD(next_id::text, 6, '0');
        
        -- Insérer dans la table de mapping
        INSERT INTO public.wrk_player_mapping (original_player_id, anonymous_player_id) 
        VALUES (original_id, anonymous_id);
    END IF;
    
    RETURN anonymous_id;
END;
$$ LANGUAGE plpgsql;