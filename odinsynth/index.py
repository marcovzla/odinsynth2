from __future__ import annotations
import os
import glob
import random
from pathlib import Path
from typing import Optional, Union
from odinson.gateway import *
from odinson.gateway.engine import ExtractorEngine
from odinson.gateway.results import ScoreDoc
from odinson.ruleutils.queryast import *

class IndexedCorpus:
    def __init__(self, ee: ExtractorEngine, docs_dir: Union[str, Path]):
        self.ee = ee
        self.docs_dir = str(docs_dir)

    @classmethod
    def from_data_dir(cls, data_dir: Union[str, Path], gw: OdinsonGateway) -> IndexedCorpus:
        """
        Makes an IndexedCorpus for data stored in `data_dir`.
        """
        data_dir = Path(data_dir)
        docs_dir = data_dir/'docs'
        index_dir = data_dir/'index'
        ee = gw.open_index(str(index_dir))
        return cls(ee, docs_dir)

    def search(self, pattern: Union[str, AstNode], max_hits: Optional[int] = None):
        """
        Searches the pattern in the index and returns the results.
        """
        return self.ee.search(str(pattern), max_hits=max_hits)

    def get_document(self, doc: Union[int, ScoreDoc]) -> Document:
        """
        Returns the Document object corresponding to the provided ScoreDoc.
        """
        lucene_doc = self._get_lucence_doc(doc)
        doc_id = lucene_doc.get('docId')
        return self._get_document(doc_id)

    def random_document(self) -> Document:
        """
        Opens a random document from our collection.
        """
        # two letter directory, e.g., AA, CD, FJ
        fold1 = random.choice(glob.glob(os.path.join(self.docs_dir, '*')))
        # directory wiki_?? where ? is a digit
        fold2 = random.choice(glob.glob(os.path.join(fold1, '*')))
        filename = random.choice(glob.glob(os.path.join(fold2, '*-doc.json.gz')))
        return Document.from_file(filename)

    def get_sentence(self, doc: Union[int, ScoreDoc]) -> Sentence:
        """
        Returns the Sentence object corresponding to the provided ScoreDoc.
        """
        lucene_doc = self._get_lucence_doc(doc)
        doc_id = lucene_doc.get('docId')
        sent_id = int(lucene_doc.get('sentId'))
        return self._get_document(doc_id).sentences[sent_id] 

    def random_sentence(self, doc: Optional[Document] = None) -> Sentence:
        """
        Returns a random sentence from the given document.
        If no document is given, then returns a random sentence from the whole collection.
        """
        if doc is None:
            doc = self.random_document()
        # ignore sentences that are too short
        sentences = [s for s in doc.sentences if s.numTokens > 3]
        return random.choice(sentences)

    def _get_lucence_doc(self, doc: Union[int, ScoreDoc]):
        """
        Returns the lucene document corresponding to the provided ScoreDoc.
        """
        lucene_doc_id = doc.doc if isinstance(doc, ScoreDoc) else doc
        lucene_doc = self.ee.extractor_engine.doc(lucene_doc_id)
        return lucene_doc

    def _get_document(self, doc_id: str) -> Document:
        """
        Gets a document id and returns the corresponding document.
        """
        fs = glob.glob(os.path.join(self.docs_dir, '**', f'{doc_id}-doc.json.gz'), recursive=True)
        if len(fs) != 1:
            raise Exception(f'{len(fs)} documents found for {doc_id=}')
        return Document.from_file(fs[0])
