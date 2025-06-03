DROP TABLE IF EXISTS public.dwh_cards;

CREATE TABLE public.dwh_cards AS
  SELECT DISTINCT a.card_type, a.card_name, a.card_url, a.card_saison, a.card_booster, b.sous_type, b.hp, b.evolving_stage, b.evolves_from, b.competence_1_nom, b.competence_1_puissance, b.competence_2_nom, b.competence_2_puissance, b.faiblesse, b.retreat
  FROM public.wrk_decklists as a
  LEFT JOIN wrk_infocards as b ON a.card_url = b.url
