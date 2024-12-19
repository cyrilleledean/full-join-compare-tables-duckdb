# Comparer deux tables de 1 million de lignes avec SQL FULL JOIN dans DuckDB

Objectif: exploiter le comportement du FULL JOIN pour comparer les lignes de deux tables.

- Créer un environnement virtuel Python et installer les dépendances

```python
python -m venv .venv
python -m pip install --upgrade pip
python -m pip install -r .\requirements.txt 
```

- Génération d'un jeu de données de 1 million de lignes avec Faker.
- Chargement du jeu de données à l'identique dans deux tables DuckDB.
- Insertion de 3 modifications et execution de la requête de comparaison.