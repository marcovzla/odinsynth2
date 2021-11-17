#!/usr/bin/env python

# Prints a list of (document id, document path) pairs,
# one per line, separated by tab.

import argparse
from pathlib import Path
import re

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('docs_dir', type=Path)
    args = parser.parse_args()
    for doc in args.docs_dir.glob('**/*-doc.json.gz'):
        m = re.search(r'(\d+)-doc\.json\.gz$', str(doc))
        if m is not None:
            print(f'{m.group(1)}\t{doc}')
