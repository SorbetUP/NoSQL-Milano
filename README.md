# NoSQL-Milano

Projet NoSQL autour d'un dataset fictif Milano Cortina 2026, avec :

- `MongoDB` pour les documents `users` et `tweets`
- `Neo4j` pour les relations sociales `FOLLOWS`
- une execution CLI pour importer les donnees et afficher les resultats
- une interface desktop/Streamlit pour explorer le dataset

Le projet repond a un ensemble de questions de cours sur les comptages, les aggregations, les relations sociales et les discussions.

## Objectif

Le projet s'appuie sur deux jeux de donnees principaux :

- [`src/docker/data/users.json`](src/docker/data/users.json)
- [`src/docker/data/tweets.json`](src/docker/data/tweets.json)

et sur un fichier de relations sociales inventees pour Neo4j :

- [`src/docker/data/follows.csv`](src/docker/data/follows.csv)

Le but est de :

1. charger les donnees dans MongoDB
2. reconstruire un graphe social dans Neo4j
3. repondre aux questions du sujet
4. proposer une interface de consultation

## Architecture

### Backend

- [`src/app_milano/main.py`](src/app_milano/main.py) : point d'entree principal
- [`src/app_milano/app_milano.py`](src/app_milano/app_milano.py) : orchestration CLI
- [`src/app_milano/config.py`](src/app_milano/config.py) : lecture du `.env` et construction des URI
- [`src/app_milano/utils/mongo.py`](src/app_milano/utils/mongo.py) : import MongoDB, requetes et aggregations
- [`src/app_milano/utils/neo4j.py`](src/app_milano/utils/neo4j.py) : import Neo4j et requetes Cypher
- [`src/app_milano/utils/display.py`](src/app_milano/utils/display.py) : interface Streamlit/desktop et affichage terminal

### Infra

- [`src/docker/mongo/docker-compose.yml`](src/docker/mongo/docker-compose.yml)
- [`src/docker/neo4j/docker-compose.yml`](src/docker/neo4j/docker-compose.yml)

## Arborescence utile

```text
src/
  app_milano/
    main.py
    app_milano.py
    config.py
    utils/
      mongo.py
      neo4j.py
      display.py
      docker.py
  docker/
    data/
      users.json
      tweets.json
      follows.csv
    mongo/
      docker-compose.yml
    neo4j/
      docker-compose.yml
.env
requirements.txt
README.md
```

## Prerequis

- Python 3.10+
- Docker Desktop
- une connexion Docker fonctionnelle (`docker info`)

## Installation

Depuis la racine du projet :

```powershell
python -m pip install -r requirements.txt
```

## Configuration

Le projet utilise [`.env`](.env).

Variables principales :

```text
COMPOSE_PROJECT_NAME=nosql_milano
MONGO_PORT=27019
MONGO_ROOT_USERNAME=root
MONGO_ROOT_PASSWORD=root_password
MONGO_APP_DB=milano2026
MONGO_APP_USERNAME=app_user
MONGO_APP_PASSWORD=app_password
NEO4J_HTTP_PORT=7474
NEO4J_BOLT_PORT=7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4j_password
```

## Lancer le projet

### 1. Mode CLI

C'est le mode principal pour preparer les bases et afficher toutes les questions.

```powershell
python src/app_milano/main.py --cli
```

Ce mode :

- verifie que Docker est disponible
- demarre MongoDB et Neo4j
- recharge `users.json` et `tweets.json` dans MongoDB
- recree le graphe Neo4j a partir de `users.json` et `follows.csv`
- affiche les resultats des questions dans le terminal

Important :

- le chargement MongoDB vide puis recharge les collections `users` et `tweets`
- l'import Neo4j execute `MATCH (n) DETACH DELETE n` puis recree le graphe

### 2. Mode interface desktop

```powershell
python src/app_milano/main.py
```

Ce mode lance l'interface Streamlit dans une fenetre desktop via `pywebview`.

Remarque importante :

- ce mode ne prepare pas les bases a lui seul
- pour avoir MongoDB et Neo4j alimentes, il faut d'abord lancer le mode CLI

Si MongoDB n'est pas disponible, une partie de l'interface retombe sur les JSON locaux.

## Acces aux bases

### MongoDB

URI applicative :

```text
mongodb://app_user:app_password@localhost:27019/milano2026?authSource=milano2026
```

### Neo4j

- Browser : `http://localhost:7474`
- Bolt : `bolt://localhost:7687`
- User : `neo4j`
- Password : `neo4j_password`

## Questions couvertes

### MongoDB

Questions gerees dans [`src/app_milano/utils/mongo.py`](src/app_milano/utils/mongo.py) :

- Q1 : nombre d'utilisateurs
- Q2 : nombre de tweets
- Q3 : nombre de hashtags distincts
- Q4 : nombre de tweets contenant un hashtag donne
- Q5 : nombre d'utilisateurs distincts ayant tweete avec un hashtag donne
- Q6 : tweets qui sont des reponses a un autre tweet
- Q12 : top 10 tweets les plus populaires
- Q13 : top 10 hashtags les plus populaires
- Q14 : tweets qui initient une discussion
- Q15 : discussion la plus longue
- Q16 : debut et fin de chaque conversation

### Neo4j

Questions gerees dans [`src/app_milano/utils/neo4j.py`](src/app_milano/utils/neo4j.py) :

- Q7 : followers de `MilanoOps`
- Q8 : utilisateurs suivis par `MilanoOps`
- Q9 : relations reciproques avec `MilanoOps`
- Q10 : utilisateurs avec plus de 10 followers
- Q11 : utilisateurs qui suivent plus de 5 utilisateurs

Note :

- les questions 14 a 16 sont actuellement traitees cote MongoDB
- les fonctions Neo4j Q14/Q15/Q16 sont encore des placeholders

## Interface

L'interface propose plusieurs vues :

- `Accueil` : KPI et apercu general
- `Top 10` : tweets et hashtags populaires
- `Recherche` : recherche utilisateur, hashtag ou texte
- `Profil` : detail d'un utilisateur et de ses tweets
- `Hashtag` : focus sur Q4 et Q5
- `Reponses` : Q6 + exploration de threads
- `Reseau` : questions Neo4j autour de `MilanoOps`

Le fichier principal de l'interface est [`src/app_milano/utils/display.py`](src/app_milano/utils/display.py).

## Donnees et modele

### MongoDB

Collections :

- `users`
- `tweets`

Champs notables :

- `users.user_id`
- `users.username`
- `users.role`
- `users.country`
- `tweets.tweet_id`
- `tweets.user_id`
- `tweets.hashtags`
- `tweets.favorite_count`
- `tweets.in_reply_to_tweet_id`

### Neo4j

Noeud :

- `(:User {user_id, username, role, country, created_at})`

Relation :

- `(:User)-[:FOLLOWS]->(:User)`

## Commandes utiles

Verifier Docker :

```powershell
docker info
```

Lancer seulement MongoDB :

```powershell
docker compose --env-file .env -f src/docker/mongo/docker-compose.yml up -d
```

Lancer seulement Neo4j :

```powershell
docker compose --env-file .env -f src/docker/neo4j/docker-compose.yml up -d
```

Arreter les services :

```powershell
docker compose --env-file .env -f src/docker/mongo/docker-compose.yml down
docker compose --env-file .env -f src/docker/neo4j/docker-compose.yml down
```

## Limites actuelles

- pas de suite de tests automatisee
- Neo4j sert aujourd'hui surtout aux questions 7 a 11
- les relations sociales viennent d'un fichier `follows.csv` invente pour le projet
- l'interface desktop suppose que les donnees aient deja ete preparees par le mode CLI

## Resume rapide

Pour une execution complete :

```powershell
python -m pip install -r requirements.txt
python src/app_milano/main.py --cli
python src/app_milano/main.py
```

Ordre recommande :

1. `--cli` pour preparer MongoDB et Neo4j
2. mode desktop pour la demo et l'exploration visuelle
