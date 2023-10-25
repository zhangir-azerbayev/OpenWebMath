import argparse
from functools import partial

from datasets import load_dataset, Dataset
import sentencepiece as spm 

import code

def batch_tokenize(batch, sp):
    num_toks = sum(map(len, sp.encode_as_ids(batch["text"])))

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

    prefiltered_owm = owm.filter(
        lambda doc: any(x in doc["url"] for x in args.domains), 
        num_proc=args.cpus
    )

    sp = spm.SentencePieceProcessor(model_file=args.tokenizer_model)
    
    summary = ""
    for dump, domain in zip(args.dumps, args.domains):
        print(f"loading {dump}...")
        data = load_dataset(dump)["train"]

        print(f"tokenizing {dump}...")
        dump_toks = get_num_toks(data, sp, args.cpus)

        print("filtered and tokenizing owm...")
        domain_dataset = prefiltered_owm.filter(lambda x: domain in x["url"])
        domain_toks = get_num_toks(domain_dataset, sp, args.cpus)

        ratio = dump_toks/domain_toks


        summary += f"{domain}\ninternet archive tokens: {dump_toks:.4E}\nowm tokens: {domain_toks:.4E}"
        summary += f"ratio: {ratio:.4f}\n\n"

    print("\n"*5, summary)


if __name__=="__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
            "--dumps", 
            nargs='+', type=str,
            default=[
                # "proofwiki", can't find a recent proofwiki xml dump
                "math_stack_exchange",
                "math_overflow",
            ]
    )
    parser.add_argument(
            "--domains", 
            nargs='+', type=str,
            default=[
                # "proofwiki.org", can't find a recent proofwiki xml dump
                "math.stackexchange.com",
                "mathoverflow.net",
            ]
    )
    parser.add_argument("--tokenizer_model", type=str)
    parser.add_argument("--cpus", type=int)

    args = parser.parse_args()

    main(args)
