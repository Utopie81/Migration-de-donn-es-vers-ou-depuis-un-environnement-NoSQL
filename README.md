# Migration-de-donn-es-vers-ou-depuis-un-environnement-NoSQL
Description

Ce projet a été réalisé dans le cadre d'une SAÉ ayant pour objectif de migrer une base de données relationnelle (SQLite) vers une base de données NoSQL orientée graphe (Neo4j).

Les données utilisées concernent les crimes et délits enregistrés par la Police et la Gendarmerie en France.
Le projet comprend l'importation des données, la création d'une base relationnelle, puis la migration vers un modèle graphe afin de faciliter l'analyse des relations entre les données.

##Travail réalisé

- Analyse des données sources

- Création d’un modèle relationnel

- Importation et nettoyage des données avec Python

- Migration de la base SQLite vers Neo4j

- Création des nœuds et relations dans Neo4j

- Rédaction du rapport du projet

##Contenu du dépôt

- MCD.png
  Modèle conceptuel de données utilisé pour la base relationnelle.

- importation db.py
  Script Python permettant d'importer les fichiers CSV dans la base SQLite.

- Migration.py
  Script Python permettant de migrer les données de SQLite vers Neo4j.

- Rapport Sae Migration.pdf

README.md
Description du projet.
