"""Code réutilisable du pipeline de détection de fraude.

Ce paquet contient la logique métier *testable*, séparée des notebooks :

- ``config``   : nomenclature Unity Catalog, colonnes clés, sel de pseudonymisation
- ``schemas``  : schéma Spark explicite et contrat de données de la source (étape 1)
- ``quality``  : moteur générique de règles de qualité et de quarantaine
- ``silver``   : transformation Bronze → Silver, règles et dictionnaire (étape 2)
- ``features`` : construction des variables comportementales (étape 3)

Pourquoi séparer le code des notebooks ? Parce qu'un notebook n'est pas
testable ni réutilisable. En isolant les fonctions ici, on peut les couvrir
par des tests unitaires et les appeler indifféremment depuis un notebook, un
job orchestré ou un futur traitement en streaming.
"""

__version__ = "0.1.0"
