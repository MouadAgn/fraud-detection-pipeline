"""Code réutilisable du pipeline de détection de fraude.

Ce paquet contient la logique métier *testable*, séparée des notebooks :

- ``config``   : nomenclature Unity Catalog et colonnes clés
- ``schemas``  : schémas Spark explicites des tables (étape 1)
- ``quality``  : règles de contrôle qualité et de quarantaine (étape 2)
- ``features`` : construction des variables comportementales (étape 3)

Pourquoi séparer le code des notebooks ? Parce qu'un notebook n'est pas
testable ni réutilisable. En isolant les fonctions ici, on peut les couvrir
par des tests unitaires et les appeler indifféremment depuis un notebook, un
job orchestré ou un futur traitement en streaming.
"""

__version__ = "0.1.0"
