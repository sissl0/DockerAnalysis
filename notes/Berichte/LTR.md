# Learning to rank
- Rankings unterschiedlich groß
- Abstände zwischen Ranks nicht gleich
### Warum Python:
- Con: Transformieren des Datensatz, Exportieren der Weights
- Pro: GO hat keine nativen LTR Modelle
### Warum LTR: 
- lin. Regression nicht möglich, da metrisch
- ord. Regression schlechter, da nicht pairwise
### Tweaks
- Ranking min. Size > 40, da Re-Ranking möglich sein muss, nicht nur TOPs -> 20048 Rankings verbleibend
- Baum 127 Blätter statt 31(Standard), da 1815530 Samples
- star_count, pull_count minmax scaled (standard praxis)
- Train: 0.8, Test: 0.2
- is_offical, is_automated als categorial features
- repo_name_cat nicht, da Kategorien unterschiedlich gut 
- Restliche Params standard, mb noch erklären falls zu wenig Inhalt
### Results
#### 50 splittet, balanced, 49200 Queries, 1474748 Einträge
##### ndgc, 0.03, 63
Spearman Rank Correlation (avg): 0.7150
Full NDCG (avg): 0.9529
Same Scores for same Features: 0.1057
##### average_precision, 0.03, 63
Spearman Rank Correlation (avg): 0.7150
Full NDCG (avg): 0.9529
Same Scores for same Features: 0.1057
##### auc, 0.03, 63
Spearman Rank Correlation (avg): 0.7150
Full NDCG (avg): 0.9529
Same Scores for same Features: 0.1057
##### ndgc, 0.05, 77
Spearman Rank Correlation (avg): 0.7367
Full NDCG (avg): 0.9571
Same Scores for same Features: 0.0863
##### average_precision, 0.05, 77
Spearman Rank Correlation (avg): 0.7367
Full NDCG (avg): 0.9571
Same Scores for same Features: 0.0863

#### 50 splittet, balanced, no remainder, 36080 Queries, 1082465 Einträge
##### ndgc, 0.05, 77
Spearman Rank Correlation (avg): 0.7995
Full NDCG (avg): 0.9681
Same Scores for same Features: 0.0605

#### balanced 100, 26936, 1482942 Einträge
##### ndgc, 0.05, 77
Spearman Rank Correlation (avg): 0.8613
Full NDCG (avg): 0.9825
Same Scores for same Features: 0.0891 

#### BERT vs Categories
##### Standard
- BERT
- Spearman Rank Correlation (avg): 0.8897
- Full NDCG (avg): 0.9893
- Same Scores for same Features: 0.0786
- Category

#### All Text Features
##### Everything
Spearman Rank Correlation (avg): 0.8353
Full NDCG (avg): 0.9786
Same Scores for same Features: 0.0401
##### -Fasttext
Spearman Rank Correlation (avg): 0.8394
Full NDCG (avg): 0.9793
Same Scores for same Features: 0.0384
##### -Fasttext, -Jaccard
Spearman Rank Correlation (avg): 0.8414
Full NDCG (avg): 0.9796
Same Scores for same Features: 0.0440
##### -Fasttext, -Jaccard, -TDIFD
Spearman Rank Correlation (avg): 0.8502
Full NDCG (avg): 0.9806
Same Scores for same Features: 0.0377
##### -Fasttext, -Jaccard, -TDIFD - Levenstein
Spearman Rank Correlation (avg): 0.8582
Full NDCG (avg): 0.9819
Same Scores for same Features: 0.0481

#### Beste Ergebnisse
star_count, pull_count, is_automated, is_offical, significant_levenshtein, significant_category, significant_jaccard, is_standalone, query_find
Spearman Rank Correlation (avg): 0.9004
Full NDCG (avg): 0.9894
Same Scores for same Features: 0.0862
### Zu GO exportieren
- Keine Unterstützung für lambdarank bei OONX, PMML, m2cgen
- -> Rest API, da hier Geschwindigkeit noch nicht so wichtig