"""
Georg Heindl
Das LTR Modell kann nicht direkt in GO geladen werden, daher wird ein kleiner API-Service in Python
gestartet, der das Modell lädt und Vorhersagen macht.
Die API erwartet eine POST-Anfrage mit einem JSON-Payload, der eine Liste von Repositories [RepoInput] enthält.
Die Antwort ist eine Liste von Scores für jedes Repository.
FastAPI für API und uvicorn für Endpunkt.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import lightgbm as lgb
import numpy as np
import uvicorn
from typing import List

model = lgb.Booster(model_file="docker_ranker.txt")

FEATURE_ORDER = ['star_count', 'pull_count', 'is_official', 'is_automated', 'significant_levenshtein', 'significant_position', 'significant_category', 'significant_jaccard', 'query_find',
            'is_standalone']

class RepoInput(BaseModel):
    repo_name_cat: float
    star_count: float
    pull_count: float
    is_official: int
    is_automated: int
    rank: int

app = FastAPI()

@app.post("/predict")
def predict(payload: dict):
    repos = payload.get("repos", [])
    feature_matrix = []
    for repo in repos:
        row = [repo[key] for key in FEATURE_ORDER]
        feature_matrix.append(row)
    feature_matrix_np = np.array(feature_matrix)
    
    predictions = model.predict(feature_matrix_np)
    comb = zip(repos, predictions)
    return [{
        "score": float(score),
        "rank": repo["rank"],
        
    } for i, (repo, score) in enumerate(comb)]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
