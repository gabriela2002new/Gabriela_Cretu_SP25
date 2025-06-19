"""
Module for preparing inverted indexes based on uploaded documents
"""
import argparse
import string
import sys
from argparse import ArgumentParser, ArgumentTypeError, FileType
from io import TextIOWrapper
from typing import Dict, List
import json

DEFAULT_PATH_TO_STORE_INVERTED_INDEX = "inverted.index"
PATH_TO_STOP_WORDS=r"C:\Users\acer\Desktop\Data Analytics Engineering Training\Python-Course\Final_Task\stop_words_en.txt"

class EncodedFileType(FileType):
    """File encoder"""

    def __call__(self, string):
        # the special argument "-" means sys.std{in,out}
        if string == "-":
            if "r" in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, encoding=self._encoding)
                return stdin
            if "w" in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, encoding=self._encoding)
                return stdout
            msg = 'argument "-" with mode %r' % self._mode
            raise ValueError(msg)

        # all other arguments are used as file names
        try:
            return open(string, self._mode, self._bufsize, self._encoding, self._errors)
        except OSError as exception:
            args = {"filename": string, "error": exception}
            message = "can't open '%(filename)s': %(error)s"
            raise ArgumentTypeError(message % args)

    def print_encoder(self):
        """printer of encoder"""
        print(self._encoding)


class InvertedIndex:
    """
    This module is necessary to extract inverted indexes from documents.
    """

    def __init__(self, words_ids: Dict[str, List[int]]):
        self.words_ids=words_ids

    def query(self, words: List[str]) -> List[int]:
        """Return the list of relevant documents for the given query"""
        if not words:
            return[]
        result=set(self.words_ids.get(words[0],[]))
        for word in words[1:]:
            result&=set(self.words_ids.get(word,[]))
        return sorted(result)

    def dump(self, filepath: str) -> None:
        """
        Allow us to write inverted indexes documents to temporary directory or local storage
        :param filepath: path to file with documents
        :return: None
        """
        with open(filepath,'w', encoding='utf-8') as f:
            json.dump(self.words_ids,f)

    @classmethod
    def load(cls, filepath: str):
        """
        Allow us to upload inverted indexes from either temporary directory or local storage
        :param filepath: path to file with documents
        :return: InvertedIndex
        """
        with open(filepath, 'r', encoding='utf-8') as f:
              data=json.load(f)
        return cls(data)

def load_documents(filepath: str) -> Dict[int, str]:
    """
    Allow us to upload documents from either tempopary directory or local storage
    :param filepath: path to file with documents
    :return: Dict[int, str]
    """
    documents = {}
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            if "\t" in line:
                doc_id, content = line.strip().lower().split("\t", 1)
                documents[int(doc_id)] = content
    return documents


import string
from typing import Dict


def build_inverted_index(documents: Dict[int, str]) -> InvertedIndex:
    """
    Builder of inverted indexes based on documents
    :param documents: dict with documents, keys are doc IDs, values are file paths
    :return: InvertedIndex instance
    """
    with open(PATH_TO_STOP_WORDS) as f:
        stop_words = [line.strip().lower() for line in f]



    inverted = {}
    for doc_id, text in documents.items():
        text = text.lower()
        text = text.translate(str.maketrans('', '', string.punctuation))
        words = text.strip().split()

        for word in words:
            if word in stop_words:
                continue
            if word not in inverted:
                inverted[word] = set()
            inverted[word].add(doc_id)

    # Convert sets to lists for JSON serializability
    inverted = {word: list(doc_ids) for word, doc_ids in inverted.items()}

    return InvertedIndex(inverted)


def callback_build(arguments) -> None:
    """process build runner"""
    return process_build(arguments.dataset, arguments.output)


def process_build(dataset, output) -> None:
    """
    Function is responsible for running of a pipeline to load documents,
    build and save inverted index.
    :param arguments: key/value pairs of arguments from 'build' subparser
    :return: None
    """
    documents: Dict[int, str] = load_documents(dataset)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(output)


def callback_query(arguments) -> None:
    """ "callback query runner"""
    process_query(arguments.query, arguments.index)


def process_query(queries, index) -> None:
    """
    Function is responsible for loading inverted indexes
    and printing document indexes for key words from arguments.query
    :param arguments: key/value pairs of arguments from 'query' subparser
    :return: None
    """
    inverted_index = InvertedIndex.load(index)
    for query in queries:
        print(query[0])
        if isinstance(query, str):
            query = query.strip().split()

        doc_indexes = ",".join(str(value) for value in inverted_index.query(query))
        print(doc_indexes)


def setup_subparsers(parser) -> None:
    """
    Initial subparsers with arguments.
    :param parser: Instance of ArgumentParser
    """
    subparser = parser.add_subparsers(dest="command")
    build_parser = subparser.add_parser(
        "build",
        help="this parser is need to load, build"
        " and save inverted index bases on documents",
    )
    build_parser.add_argument(
        "-d",
        "--dataset",
        required=True,
        help="You should specify path to file with documents. "
    )
    build_parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help="You should specify path to save inverted index. "
        "The default: %(default)s",
    )
    build_parser.set_defaults(callback=callback_build)

    query_parser = subparser.add_parser(
        "query", help="This parser is need to load and apply inverted index"
    )
    query_parser.add_argument(
        "--index",
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help="specify the path where inverted indexes are. " "The default: %(default)s"
    )
    query_file_group = query_parser.add_mutually_exclusive_group(required=True)
    query_file_group.add_argument(
        "-q",
        "--query",
        dest="query",
        action="append",
        nargs="+",
        help="you can specify a sequence of queries to process them overall"
    )
    query_file_group.add_argument(
        "--query_from_file",
        dest="query",
        type=EncodedFileType("r", encoding="utf-8"),
        # default=TextIOWrapper(sys.stdin.buffer, encoding='utf-8'),
        help="query file to get queries for inverted index"
    )
    query_parser.set_defaults(callback=callback_query)

def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inverted index CLI")
    setup_subparsers(parser)
    return parser

def main():
    """
    Starter of the pipeline
    """
    parser = setup_parser()
    args = parser.parse_args()

    if hasattr(args, "callback"):
        args.callback(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
