Alte Repos haben altes Manifest Schema und meine Requests bekommen Error 400.
Die alte Registry api v1 existiert nicht mehr 410, deshalb können die Layer dieser Repos nicht mehr erreicht werden.
Das ist schlecht, da es sich teils um Repos mit vielen Pulls handelt.

### Sort all layers
sort -u -t$'\t' -k1,1 all_layers.tsv > unique_layers.tsv
### Sum up size
awk -F'\t' '{sum += $3} END {print sum/1e12 " TB"}' unique_layers.tsv
10391.8 TB
### TSV to JSONL
awk -F'\t' '{printf("{\"layer\":\"%s\",\"repo\":\"%s\",\"size\":%s}\n",$1,$2,$3)}' unique_layers.tsv > unique_layer_list.jsonl
