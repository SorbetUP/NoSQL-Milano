# NoSQL-Milano

Version simple du projet avec une seule commande Python pour tout lancer.

## Fichiers utiles

- `data/users.json`
- `data/tweets.json`
- `docker-compose.yml`
- `bootstrap.py`
- `.env`

## Installation

```bash
python3 -m pip install -r requirements.txt
python3 bootstrap.py
```

## Ce que fait `bootstrap.py`

- verifie Docker
- demarre MongoDB et Neo4j
- attend que les deux bases soient pretes
- cree la base MongoDB `milano2026`
- cree les index MongoDB
- importe `data/users.json` et `data/tweets.json`
- cree les contraintes Neo4j
- importe les noeuds `User` et `Tweet`
- cree les relations `AUTHORED` et `REPLY_TO`
- affiche les infos de connexion

## Connexions

MongoDB :

```text
mongodb://app_user:app_password@localhost:27017/milano2026?authSource=milano2026
```

Neo4j :

```text
Browser: http://localhost:7474
Bolt: bolt://localhost:7687
User: neo4j
Password: neo4j_password
```
