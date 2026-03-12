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
mongodb://app_user:app_password@localhost:27019/milano2026?authSource=milano2026
```

Neo4j :

```text
Browser: http://localhost:7474
Bolt: bolt://localhost:7687
User: neo4j
Password: neo4j_password
```

### SorbetUP / LemmingSnow

- [x] mettre en place une premiere base du projet avec `.env`, `docker-compose.yml` et `bootstrap.py`
- [x] preparer un dataset de travail dans `data/users.json` et `data/tweets.json`
- [ ] finaliser `bootstrap.py` pour qu'il serve d'entree principale stable du projet
- [ ] relier proprement les parties MongoDB et Neo4j dans une execution unique
- [ ] traiter les questions 1, 2, 3, 12 et 13
- [ ] verifier les sorties globales de l'application pour l'ensemble des questions
- [ ] finaliser le `README` avec exemples d'execution et captures
- [ ] faire les tests finaux et preparer la demo
- [ ] rediger dans le rapport : introduction, environnement local, architecture generale, integration finale, conclusion

### Kevin Zhang

- [x] creer `mongo/import_data.py` pour l'import MongoDB
- [x] implementer le CRUD MongoDB dans `mongo/crudfunc.py`
- [ ] nettoyer et aligner `mongo/import_data.py` et `mongo/crudfunc.py` avec `.env` et le reste du projet
- [ ] traiter les questions 4, 5 et 6
- [ ] produire les visualisations MongoDB : KPI, top hashtags, top tweets
- [ ] rediger dans le rapport : modele MongoDB, CRUD, requetes MongoDB, agregations, visualisations MongoDB
- [ ] preparer les preuves Scrum cote MongoDB : sprint backlog, daily log, resultats

### Yanis GOBEREAU

- [ ] verifier la connexion Neo4j en local
- [ ] finaliser l'import Neo4j a partir du dataset MongoDB
- [ ] completer les relations `FOLLOWS` et `RETWEETS`
- [ ] traiter les questions 7, 8, 9, 10, 11, 14, 15 et 16
- [ ] produire la visualisation du reseau autour de `MilanoOps`
- [ ] rediger dans le rapport : modele Neo4j, relations graphe, requetes Cypher, discussions/threads, visualisation Neo4j
- [ ] preparer les preuves Scrum cote Neo4j : sprint backlog, daily log, resultats
