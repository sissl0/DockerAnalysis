# Entscheidungen

### Files hashen um nicht doppelt zu analysieren vs Immer alle analysieren

### Layer Digest in REDIS Set vs Alle Layer Digests abspeichern und dann Set (unique) daraus machen

### Secrets im gesamten Repo oder pro Image(Tag) 
Secrets nicht doppelt beachten

### JSONL oder (NO) -SQL

### Nur Tags downloaden, die auf neue Version hinweisen (nicht nur base image variationen)

### Durch Fallstudie Gewichtungen der Best Match Search finden, damit Semi Bruteforce möglich
Sonst würde man Ergebnisse mehrfach bekommen (Beispiel balena: in ba, bal, bale...)
-> Stop sobald query nicht mehr Standalone

### Ranking ML oder statisch
+ 36³*100 Dataset genug für ML
+ nichtlineare Effekte
- Aufwand
- Trainingsdauer
- schwer kontrollierbar