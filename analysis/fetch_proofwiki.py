import os
import sys
import re
import json
import ndjson
from pathlib import Path
import requests
import random
from tqdm import tqdm

random.seed(20)
 
def _download_with_progress_bar(url):
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    to_return = bytearray()
    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        to_return += data
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        raise AssertionError("ERROR, something went wrong")

    return to_return


PROOFWIKI_URL = (
    "https://zenodo.org/record/4902289/files/naturalproofs_proofwiki.json?download=1"
)
def proofwiki(testing=False):
    save_dir = "proofwiki"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    if testing:
        with open("naturalproofs/proofwiki.json") as f:
            struct = json.load(f)
    else:
        print("DOWNLOADING PROOFWIKI")
        resp = _download_with_progress_bar(PROOFWIKI_URL)
        struct = json.loads(resp.decode("utf-8"))
        print("DONE DOWNLOADING PROOFWIKI")

    print(struct["dataset"]["theorems"][0])

    db = []
    
    for i, thm in enumerate(tqdm(struct["dataset"]["theorems"])):
        if thm["contents"]:
            thm_string = "\\section{" + thm["label"] + "}\n"
            thm_string += (
                "Tags: " + ", ".join(thm["categories"]).replace("/", ": ") + "\n\n"
            )

            thm_string += (
                "\\begin{theorem}\n"
                + "\n".join(thm["contents"])
                + "\n\\end{theorem}\n\n"
            )

            for proof in thm["proofs"]:
                thm_string += (
                    "\\begin{proof}\n"
                    + "\n".join(proof["contents"])
                    + "\n\\end{proof}\n\n"
                )

            print(thm_string)

            db.append({"text": thm_string, "meta": dict()})

    for defn in tqdm(struct["dataset"]["definitions"]):
        if defn["contents"]:
            defn_string = (
                "\\begin{definition}["
                + defn["label"]
                + "]\n"
                + "\n".join(defn["contents"])
                + "\n\\end{definition}"
            ).strip()
            
            db.append({"text": defn_string, "meta": dict()})
    print(db)

    with open("proofwiki.jsonl", "w") as f:
        ndjson.dump(db, f)
            
            
if __name__=="__main__": 
    proofwiki()
