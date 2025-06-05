The first objective of our project will be to extract data from the TCG Pocket site.

To start with, we'll create a conda environment to import all Python dependencies and libraries and activate it: 

> conda create --name TCG_CLOV (confirm with 'y')
> conda activate TCG_CLOV

Next, we need to move to the project directory (for example, if it's located at the root of the C: drive):

> C:
> cd SAE-601-CLOV

Download pip and the Python libraries: 

> conda install pip (confirm with 'y')
> pip install pandas aiohttp aiofile psycopg psycopg_binary requests bs4 matplotlib tqdm seaborn plotly ipywidgets

We now run the program that manages all extraction steps:

> python run.py

Tournament scraping begins.
Once tournament collection is complete, card extraction starts automatically (confirm with ‘y’).
Once all the information has been retrieved, we'll integrate it into our PostgreSQL database.
Start the PostgreSQL server: this is the official native version. If you subsequently encounter an error, please contact us at e2201621@etud.univ-ubs.fr.

The program will ask you to enter the database password.

The data will then be automatically integrated into the PostgreSQL database. There's no need to define environment variables - they're built into the insertion code.
As a security measure, before retrieving the data in .csv format, the program will require you to re-enter the password.

Finally, data in the correct format will be available in data/csv. Cached data will be automatically deleted to free up storage space.
Then launch the notebook named SAE601_COCHET_LEBRETON_OUATTARA_VERLY_LAGADEC.ipynb with Jupyter.


-------------------------------------------------------------------------------



L'objectif premier de notre projet sera d'extraire les données provenant du site de TCG Pocket.

Pour commencer, nous allons créer un environnement conda afin d'importer toutes les dépendances et bibliothèques Python et l'activer : 

> conda create --name TCG_CLOV (valider avec 'y')
> conda activate TCG_CLOV

Ensuite, nous devons nous déplacer dans le répertoire du projet (par exemple, s'il est présent à la racine du lecteur C:) :

> C:
> cd SAE-601-CLOV

On télécharge pip et les bibliothèques Python : 

> conda install pip (valider avec 'y')
> pip install pandas aiohttp aiofile psycopg psycopg_binary requests bs4 matplotlib tqdm seaborn plotly ipywidgets

On lance maintenant le programme qui gère toutes les étapes d'extraction :

> python run.py

Le scraping des tournois se lance.
Une fois la collection des tournois terminée, l'extraction des cartes démarre automatiquement (valider avec 'y').
Quand toutes les informations sont récupérées, nous allons l'intégrer dans notre base PostgreSQL.
Démarrer le serveur PostgreSQL : il s'agit de la version officielle native. Si une erreur est provoquée par la suite, vous pouvez nous contacter à cette adresse : e2201621@etud.univ-ubs.fr
Le programme vous demandera de renseigner le mot de passe de la base.

Les données seront alors automatiquement intégrées dans la base PostgreSQL. Pas besoin de définir des variables d'environnement, elles sont intégrées au code d'insertion.
Par mesure de sécurité, avant la récupération en format .csv, le programme exigera que vous renseigniez à nouveau le mot de passe.

Et enfin, les données au bon format seront disponibles dans data/csv. Les données caches seront automatiquement supprimées pour libérer de l'espace de stockage.
Ensuite, lancer le notebook nommé SAE601_COCHET_LEBRETON_OUATTARA_VERLY_LAGADEC.ipynb grâce à Jupyter.