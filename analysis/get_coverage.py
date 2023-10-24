import argparse
from typing import List
from functools import partial
import sys
from concurrent.futures import ProcessPoolExecutor

import ndjson
from tqdm import tqdm
from datasets import load_dataset
import sentencepiece as spm 

import code

def is_good_url(doc: str, domains: str): return any(x in doc["url"] for x in domains)

def batch_tokenize(batch, sp):
    num_toks = sum(len(x) for x in sp.encode_as_ids([str(x) for x in batch["text"]]))

    return {"tokens": [num_toks]}

def get_num_toks(data, sp, cpus):
    my_batch_tokenize = partial(batch_tokenize, sp=sp)

    tokenized_ds = data.map(
            my_batch_tokenize,
            batched=True, 
            remove_columns=data.column_names,
            num_proc=cpus
    )

    num_toks = sum(x["tokens"] for x in tokenized_ds)
    return num_toks

def main(args):
    print("dumps: ", args.dumps)
    print("domains: ", args.domains)

    owm = load_dataset("open-web-math/open-web-math")["train"]

    is_good_url_partial = partial(is_good_url, domains=args.domains)

    filtered_owm = owm.filter(is_good_url_partial, num_proc=args.cpus)

    sp = spm.SentencePieceProcessor(model_file=args.tokenizer_model)
    
    dumps_tokens = []
    for dump in args.dumps:
        print(f"loading {dump}...")
        data = load_dataset(dump)["train"]

        print(data.column_names)

        print(f"tokenizing {dump}...")
        num_toks = get_num_toks(data, sp, args.cpus)

        print(f'{num_toks:.3e}')

        dumps_tokens.append(num_toks)

    domains_tokens = dict()
    print("tokenizing filtered owm...")
    for domain in args.domains:
        domain_dataset = filtered_owm.filter(lambda x: domain in x["url"])
        domains_tokens[domain] = get_num_toks(domain_dataset, sp, args.cpus)

    print("\n"*10)

    for i, domain in enumerate(args.domains):
        dump_toks = dumps_tokens[i]
        domain_toks = domains_tokens[domain]
        ratio = dump_toks/domain_toks


        print(f"{domain}\ninternet archive tokens: {dump_toks:.4E}\nowm tokens: {domain_toks:.4E}")
        print(f"ratio: {ratio:.4f}\n\n")


if __name__=="__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
            "--dumps", 
            nargs='+', type=str,
            default=[
                "proofwiki", 
                "math_stack_exchange",
                "math_overflow"
            ]
    )
    parser.add_argument(
            "--domains", 
            nargs='+', type=str,
            default=[
                "proofwiki.org",
                "math.stackexchange.com",
                "mathoverflow.net"
            ]
    )
    parser.add_argument("--tokenizer_model", type=str)
    parser.add_argument("--cpus", type=int)

    args = parser.parse_args()

    main(args)

