# Weight Sample Collection
Um die Gewichtung des Best Match Ranking System von Docker Search mit linearer Regression wurden 34³ Queries als Sample angefragt. Es sind 34 zulässige Zeichen für Nutzernamen. Tatsächlich sind Nutzernamen min. 4 Zeichen lang, allerdings haben manche alten noch 3. Da diese Nutzernamen in späteren Queries möglicherweise nicht mehr vorkommen werden und sowieso ein Sample benötigt wurde, wurden sie um Ressourcen auf beiden Seiten zu sparen schon jetzt gescannt. 
- Dauer ca. 43 Minuten bei 5 Proxies
- 57 von 34³ sind fehlgeschlagen wegen leeren Antworten oder Timeout exceeded
- 46608/34³ viele Rankings
- 2048288 viele Repos == größe des Samples
- 1790438 viele unique Repos 
- 30.5 MB komprimiert
Total queries: 46653
Total repos: 2062188
Unique repos: 1805835
