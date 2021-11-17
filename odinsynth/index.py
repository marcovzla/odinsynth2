from __future__ import annotations
import random
from pathlib import Path
from typing import Optional, Union
from odinson.gateway import *
from odinson.gateway.engine import ExtractorEngine
from odinson.gateway.results import ScoreDoc
from odinson.ruleutils.queryast import *
from .util import read_tsv_mapping

class IndexedCorpus:
    def __init__(self, ee: ExtractorEngine, docs_dir: Union[str, Path], docs_index: map[str, str]):
        self.ee = ee
        self.docs_dir = Path(docs_dir)
        self.docs_index = docs_index

    @classmethod
    def from_data_dir(cls, data_dir: Union[str, Path], gw: OdinsonGateway) -> IndexedCorpus:
        """
        Makes an IndexedCorpus for data stored in `data_dir`.
        """
        data_dir = Path(data_dir)
        docs_dir = data_dir/'docs'
        index_dir = data_dir/'index'
        ee = gw.open_index(index_dir)
        docs_index = read_tsv_mapping(docs_dir/'documents.tsv')
        return cls(ee, docs_dir, docs_index)

    def get_results(self, query: Union[str, AstNode], max_hits=None):
        matches = []
        results = self.search(query, max_hits)
        for doc in results.docs:
            if len(doc.matches) != 1:
                continue
            sent = self.get_sentence(doc)
            m = doc.matches[0]
            span = (m.start, m.end)
            matches.append(dict(match=span, sentence=sent.to_dict()))
        return {
            'query': str(query),
            'num_matches': len(matches),
            'docs_dir': str(self.docs_dir),
            'matches': matches,
        }

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
        doc_id = random.choice(list(self.docs_index.keys()))
        filename = self.docs_index[doc_id]
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
        return Document.from_file(self.docs_index[doc_id])
