# NF26 ― P2021 ― Projet

Les sujets et consignes sont consultables en ligne ici:

https://gitlab.utc.fr/NF26/tf-managed/TDs-p2021/nf26-projects/-/blob/main/README.md

## Structuration du dépôt

Ce dépôt reprend une structure minimale de projet, avec:

 - un dossier `project` pour votre code.
 - un `Pipfile` et son `Pipfile.lock`, contenant une liste des dépendances pour créer un environnement virtuel grâce à `make init`.
 - un `Makefile` simple avec quelques cibles pour le style de votre code.
 - un setup de `pre-commit` simple (`.pre-commit-config.yaml`).

Vous pouvez adapter ces fichiers à votre guise pour votre rendu.

## Documentation pour reproductibilité de votre projet

 - Se connecter au serveur `nf26-2.leger.tf`.
 - Se rendre dans le dossier `/home/damsebas/nf26-project-damsebas`.
 - Lancer `make init`

Pour télécharger le jeu de données Safecast Radiation Measurements de Kaggle:

 - Suivre les étapes du tutoriel : https://wdeback.gitlab.io/post/2018-03-08-how-to-download-kaggle-dataset-from-command-line/
 Il faudra renseigner le lien de téléchargement : https://www.kaggle.com/safecast/safecast/download
 - Renommer le nouveau dossier obtenu en `data`
 - Renommer le fichier download se trouvant dans `data/safecard/safecard` en measurements.csv.zip
 - Déplacer le fichier `measurements.csv.zip` dans `data`
 - Dézipper avec la commande `unzip measurements.csv.zip`

Création et alimentation de la base Cassandra:
 - Pour créer le keyspace et insérer les données, lancer `make init-keyspace` puis `make insert` (A ne pas faire si on veut trouver les mêmes résultats que dans le rapport !)

Requêtes:
 - Distribution et zones les plus radioactives: lancer `make run`
 - Aspect temporel : lancer `make spark`
