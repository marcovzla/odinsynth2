#!/usr/bin/env python

import json
import argparse
import uuid
from pathlib import Path
import threading
import _thread as thread
from odinson.gateway import OdinsonGateway
from odinson.ruleutils.queryast import FieldConstraint, NotConstraint, RepeatSurface, TokenSurface
from odinsynth.rulegen import RuleGeneration
from odinsynth.index import IndexedCorpus


def quit_function():
    thread.interrupt_main()


def wait_for_function(seconds: int, func, *args, **kwargs):
    """
    Tries to return a random surface rule, unless it runs out of time.
    """
    timer = threading.Timer(seconds, quit_function)
    timer.start()
    try:
        return func(*args, **kwargs)
    except KeyboardInterrupt:
        return None
    finally:
        timer.cancel()

def validate_query(query):
    if query is None:
        return False
    if isinstance(query, RepeatSurface) and query.min == 0:
        # e.g. [word=car]?
        return False
    if (isinstance(query, TokenSurface)
        and isinstance(query.constraint, NotConstraint)
        and isinstance(query.constraint.constraint, FieldConstraint)):
        # e.g. [!word=car]
        return False
    return True


if __name__ == '__main__':
    # command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--out-dir', type=Path)
    parser.add_argument('--data-dir', type=Path, default=Path('/media/data1/odinsynth'))
    parser.add_argument('--mini-data-dir', type=Path, default=Path('/media/data1/odinsynth-mini'))
    parser.add_argument('--hybrid', action='store_true')
    parser.add_argument('--num-queries', type=int, default=10)
    parser.add_argument('--num-matches', type=int, default=100)
    args = parser.parse_args()
    # ensure output dir exists
    if not args.out_dir.exists():
        args.out_dir.mkdir()
    # start system
    gw = OdinsonGateway.launch(javaopts=['-Xmx10g'])
    gen = RuleGeneration.from_data_dir(args.mini_data_dir, gw)
    gen_rule = gen.random_hybrid_rule if args.hybrid else gen.random_surface_rule
    corpus = IndexedCorpus.from_data_dir(args.data_dir, gw)
    # generate queries
    for i in range(args.num_queries):
        print(f'{i+1}/{args.num_queries}')
        print('  generating random query')
        query = gen_rule()
        print('  query:', query)
        if not validate_query(query):
            print('  rejected')
            continue
        print('  searching')
        data = wait_for_function(5, corpus.get_results, query, args.num_matches)
        if data is None:
            print('  timeout')
            continue
        print(f'  {data["num_matches"]} sentences found')
        if data['num_matches'] > 0:
            print('  saving results')
            with open(args.out_dir/f'query_{uuid.uuid4()}.json', 'w') as f:
                json.dump(data, f)
