# E(L)T, Datawarehouse, OLAP et SQL avancé

A Modern Data Stack with (`Meltano`,) `dbt`, `DuckDB` (and `Evidence`) !

---

## Préambule

### Jeux de données

- [ ] [Données éCO2mix régionales consolidées et définitives (janvier 2013 à janvier 2023)](https://opendata.reseaux-energies.fr/explore/dataset/eco2mix-regional-cons-def/) mises à disposition sur le [portail opendata réseaux-énergies](https://opendata.reseaux-energies.fr/pages/accueil/) (ODRÉ).

Les données éCO2mix sont issues de [l'application RTE](https://www.rte-france.com/eco2mix). En outre, l'Agence ORE se présente comme [Un portail de dataviz des données de l'énergie](https://www.agenceore.fr/) et propose des visus de plusieurs jeux de données disponibles sur le portail opendata réseaux-énergies.

Une extraction du jeu de données éCO2mix, en date du 27 nov 2024, est [disponible sur uncloud](https://uncloud.univ-nantes.fr/index.php/s/fzeDmkTM2YN9Bya/download/eco2mix-regional-cons-def.csv) : privilégier cette source.

- [ ] le jeu de données [éCO2mix régional temps réel](https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-tr/) couvre les derniers mois publiés (depuis janvier 2023) dans une version préalable. Les données sont ensuite consolidées et validées pour produire le jeu de données précédent.
  
Là encore, une copie du jeu de données est [disponible sur uncloud](https://uncloud.univ-nantes.fr/index.php/s/XBLZmDQtkitaA64/download/eco2mix-regional-tr.csv).

- [ ] un jeu de données complémentaire, le [relevé des températures quotidiennes régionales (depuis janvier 2016)](https://odre.opendatasoft.com/explore/dataset/temperature-quotidienne-regionale/), servira dans la seconde partie du TP.

La [copie uncloud](https://uncloud.univ-nantes.fr/index.php/s/JXTXEeEWSNXx3Di/download/temperature-quotidienne-regionale.csv).

### Outils et technologies

SQL, DuckDB, dbt, Evidence (?opt.) et Meltano (?opt.)

- la documentation de [DuckDB](https://duckdb.org/docs/)
- la documentation de [dbt](https://docs.getdbt.com/docs/introduction)
- un site dédié à [SQL](https://sql.sh)
- la documentation d'[Evidence](https://docs.evidence.dev)
- la documentation de [Meltano](https://docs.meltano.com)

### Livrable

Un projet `dbt` intégral dans lequel figurent des modèles de données (définis par des transformations), des tests et de la documentation, plus tous les objets utiles au projet. Le code doit pouvoir être rejoué sans erreur, du début à la fin. Le fichier `README.md`, à la racine du projet, doit livrer les instructions pour la mise en route du projet, et doit expliquer les choix réalisés.

Si d'autres outils sont utilisés au cours du projet (`Evidence` et `Meltano` par exemple), il est judicieux de constituer un `monorepo` à partir du projet `dbt`.

---

## 0. Mise en place de l'environnement de travail

### éco-système Python, dbt et ducdkb

Dans le Terminal Linux, créer un répertoire racine `<OLAP_PRJ>` (nom quelconque) pour tout le TP (données, projet dbt, etc.) et y installer un environnement virtuel Python avec le package `dbt-duckdb`.

```bash
prompt$ mkdir <OLAP_PRJ>
prompt$ cd <OLAP_PRJ>
prompt$ python -m venv venv
prompt$ source venv/bin/activate
prompt$ pip install dbt-duckdb
```

Si le tuyau est bouché, penser au proxy:

```bash
prompt$ export HTTPS_PROXY=https://cache.etu.univ-nantes.fr:3128
```

### Travailler avec DuckDB

Une façon indémodable de travailler avec le SGBD-R `DuckDB`, c'est
d'installer le client en ligne de commande (CLI), disponible à l'url
<https://duckdb.org/docs/installation/>.

Puis dans le Terminal:

```bash
prompt$ duckdb
D select 42;
D .quit
```

Une introduction au CLI `duckdb` est disponible à l'url <https://duckdb.org/docs/api/cli/overview.html>. 

### Démarrer un projet `dbt`

Initialiser un projet dbt dans le répertoire `./transform/` avec la commande :

```bash
prompt$ dbt init transform
```

Observer la structure du projet `dbt` :

```bash
prompt$ tree transform
```

Puis exécuter le projet dans sa version initiale :

```bash
prompt$ cd transform
prompt$ dbt run
```

Observer les requêtes SQL, définies dans le répertoire `models/example/` et compilées dans le répertoire `target/run/`. En outre, un nouveau fichier `dev.duckdb` a été créé, qui contient la base de données DuckDB produite par les transformations. Vous pouvez explorer cette base de données avec le client `duckdb` :

```bash
prompt$ duckdb dev.duckdb
D .tables
```

Pour jouer les tests de votre projet `dbt` :

```bash
prompt$ dbt test
```

Et pour générer et parcourir la documentation :


```bash
prompt$ dbt docs generate
prompt$ dbt docs serve
```

La prise en main de `dbt` passe par la lecture de la [documentation](https://docs.getdbt.com/docs/introduction) et notamment la section [Quickstart for dbt Core](https://docs.getdbt.com/guides/manual-install).


_Tips_ : il est recommandé de versionner son projet `dbt` avec `git`.

---

## 1. Extraction et premiers pas avec le jeu de données

On considère le jeu de données principal, éCO2mix régional consolidé et définitif, de janvier 2013 à janvier 2023. Consulter les méta-données sur le portail opendata réseaux-énergie.

Quelques première questions à se poser :

- Quels sont les attributs porteurs de sens ?
- Quels sont les attributs redondants ?
- Les champs TCO et TCH sont-ils fiables ?
- À quoi correspond le pompage ? une consommation ? une production ?
- ...

__Tips__: l'extraction "raw" et le chargement initial vers la base DuckDB, à des fins exploratoires, se fait manuellement dans la base DuckDB, grâce aux fonctions natives de [lecture de csv](https://duckdb.org/docs/data/csv/overview.html). De manière alternative, il est possible de créer un projet `Meltano` pour l'extraction et le chargement. Pour les plus aventuriers, un guide de migration est disponible ici <https://docs.meltano.com/guide/migrate-an-existing-dbt-project/>.

Quoi qu'il en soit, il est recommendé de déclarer la table créée comme copie initiale du CSV en tant que source dans `dbt`.

__Remarque__: la puissance instantanée électrique (en MW) caractérise un "flux" électrique, comme une "vitesse" à un instant _t_. Pour avoir une mesure cumulée sur une durée (qu'on appelle __énergie__), il faut _multiplier_ par un temps. L'unité est donc le MWh (mégawatt-heure et non le MW/h). Par exemple, une puissance de 7 MW pendant 2 heures correspond à une énergie de 14 MWh.

## 2. Nettoyage des données

Proposer quelques premières transformations (des modèles `dbt`) pour préparer le jeu de données à l'analyse.

Envisager l'ajout d'attributs calculés tels que la production totale instantanée (en MW).

Considérer également la construction d'une vue dont le grain est journalier, pour faciliter les analyses ultérieures.

## 3. Exploration

Construire les requêtes SQL suivantes, avec éventuellement un rendu visuel (graphique). Tout _embellissement_ du résultat, dans la mesure où il améliore effectivement la lisibilité, sera apprécié (et donc valorisé).

Le résultat de chaque requête doit faire l'objet d'une brève interprétation textuelle. Si nécessaire, un complément d'investigation (une ou plusieurs requêtes supplémentaires, qui permettent une interprétation plus fine) peut être proposé pour approfondir un sujet.


1. __Groupement et agrégation simples__ : Production (en GWh) et consommation (en GWh), ainsi que leurs versions min, max et moyenne instantanées (en Mw), par mois et par région.

2. __Pivot__ (construction `CASE WHEN THEN END`) : construire une table (résultat de requête) de la consommation (GWh) journalière montrant une colonne par région. Par construction, chaque date devient une clé de la relation-résultat. Autrement dit, il y a une ligne de données de consommation pour chaque date.

3. __Fenêtre glissante__ (_window function_ avec `RANGE`) : la consommation (GWh) du mois écoulé, chaque jour.

4. __Variation__ (_window function_, avec _CTE_ pour décomposer le calcul) : les 20 plus grands écarts de consommation quotidienne (GWh), d'un jour à l'autre.

5. __Quantité cumulée__ (_window functions_ + _CTE_) : jour du dépassement des énergies renouvelables, pour chaque année (de 2013 à 2021). En d'autres termes, à quel moment de l'année (une date) la consommation atteint - dépassse - la production annuelle totale des filières renouvelables ?

6. __Calcul de point fixe__ (_CTE récursive_) : trouver toutes les périodes correspondant aux 3 plus longues séquences d'augmentation de la consommation instantanée. Voici un extrait (la première ligne) de résultat escompté :

| Date - Heure | Durée (hh:mm:ss) | 	Région    | Séquence (MW*) | Rang |
| :----------- | :---             | ---       | ---             | --- |
| 2016-07-18 02:30:00	| 11:00:00	| Île-de-France| [4616, 4646, 4661, 4715, 4942, 5009, 5391, 568...| 1 |

7. __Construction du cube__ (`GROUP BY CUBE|GROUPING SETS|ROLLUP`) : donner toutes les valeurs de consommation (en GWh) agrégés par jour, par mois, par année et sur toute la période, ainsi que par région, par zone (NO, NE, SO, SE et IdF) et sur l'ensemble du territoire métropolitain (à l'exclusion de la Corse, non représentée dans le jeu de données). Toutes les combinaisons de ces 2 dimensions (temps et géographie) doivent figurer dans le résultat.

## 4. De la zone de transit à l'entrepôt

Il est maintenant temps de concevoir et implémenter une base de données proprement multi-dimensionnelle, un entrepôt, selon un _schéma en ét_.

1. Intégrer un second jeu de données d'historique des températures (2016 à 2022), disponible sur le [portail ODRÉ](https://opendata.reseaux-energies.fr).
Résoudre les problèmes d'alignement, sur la base d'un relevé par jour. Conserver les températures minimale, maximale et moyenne.

2. Concevoir, implémenter et alimenter avec les données existantes, une base de données multi-dimensionnelle comportant les mesures de production et de consommation régionale quotidienne (GWh), de production régionale quotidienne par filière (GWh), ainsi que les taux de couverture (TCO), sur les trois dimensions : temps (jour, mois, saison, année, toute la période), géographie (région, quarts, pays), température (au degré, par intervalle (glacial=$[-\infty,0)$, froid=$[0,8)$, modéré=$[8,17)$, idéal=$[17,25]$, chaud=$[25,33)$, extrême=$[33,+\infty)$), et toutes températures confondues).

3. Proposer la requête sur ce schéma en flocon, qui permette de construire le __cuboïde__ (la vue) par mois, par quart et par intervalle de température.

4. Proposer une procédure pour la mise-à-jour incrémentale (sans recalculer l'intégralité) de l'entrepôt lorsque de nouvelles données sont disponibles, en considérant également le changement de statut de _donnée temps réel_ à _donnée consolidée_ puis _données définitives_.

## 5. [Bonus] Étude comparative des formats de stockage

1. Proposer une évaluation de performance (taille et temps de réponse) pour la lecture (intégrale ou par requête décisionnelle) et l'écriture de données de/vers `CSV`, `JSON`, `PARQUET` (compression `SNAPPY` ou `ZSTD`) et DuckDB.

2. Proposer une évaluation de performance du format `Apache Arrow` dans DuckDB. Envisager pour cela le _driver `ADBC`_.


## Ressources supplémentaires et liens (peut-être) utiles

- migrer un projet edbt vers Meltano : <https://docs.meltano.com/guide/migrate-an-existing-dbt-project/>
- exemple de MDS DuckDB + dbt : <https://motherduck.com/blog/duckdb-dbt-e2e-data-engineering-project-part-2/>
- dépôt de packages pour dbt : <https://hub.getdbt.com>
- guide de démmarage rapide avec dbt core : <https://docs.getdbt.com/guides/manual-install>
- langage de templating Ninja pour dbt : <https://docs.getdbt.com/guides/using-jinja>
- documentation CLI duckdb : <https://duckdb.org/docs/api/cli/overview.html>