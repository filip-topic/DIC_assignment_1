#!/usr/bin/env python3
"""
mr_chi_square.py – Map-Reduce job that reads a pre-computed
document-frequency file (df_counts.txt), calculates χ² for every
(token, category) pair and outputs the required top-75 lists.

USAGE
-----

# Hadoop / EMR
python mr_chi_square.py -r hadoop \
        --dfcounts path/to/df_counts.txt \
        dummy_input.txt

# Local runner
python mr_chi_square.py --dfcounts ./output/df_counts.txt dummy
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
from ast import literal_eval
import heapq
import logging
from mrjob.protocol import RawProtocol

LOGGER = logging.getLogger(__name__)


class MRChiSquare(MRJob):
    """
    One-step MRJob
    mapper_init loads the DF table into memory ONCE
    mapper emits (category, (token, χ²))
    combiner/reducer keep the largest 75 scores per category
    second step merges all selected tokens into one dictionary line
    """

    OUTPUT_PROTOCOL = RawProtocol

    TOP_K = 75

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--dfcounts', required=True,
                          help='Path to df_counts.txt produced by MRDocFreq')

    # utility
    def _safe_parse_key(self, raw):
        #Turn the literal string representation of a tuple "(token, category)" into a Python tuple safely.
        try:
            return literal_eval(raw)
        except (ValueError, SyntaxError):
            self.increment_counter('ChiSquare', 'MalformedKey', 1)
            raise

    # mapper 
    def mapper_init(self):
        
        #load DF counts into memory. The file is shipped by mrjob because we passed it via --dfcounts.
        
        df_path = self.options.dfcounts
        self.df = defaultdict(lambda: defaultdict(int))
        self.total_by_category = defaultdict(int)
        self.total_by_token = defaultdict(int)

        with open(df_path, 'r', encoding='utf-8') as f:
            for line in f:
                #handle possible BOM artefacts
                line = line.lstrip('\ufeff').rstrip('\n')
                try:
                    key_str, value_str = line.split('\t')
                    token, category = self._safe_parse_key(key_str)
                    count = int(value_str)
                except Exception as exc:               # noqa: BLE001
                    LOGGER.warning("Skip bad line: %s – %s", line, exc)
                    self.increment_counter('ChiSquare', 'BadLine', 1)
                    continue

                if token == '__TOTAL__':
                    self.total_by_category[category] += count
                elif category == '__TOTAL__':
                    self.total_by_token[token] += count
                else:
                    self.df[token][category] = count

        self.total_reviews = sum(self.total_by_category.values())
        LOGGER.info("Loaded DF counts for %d tokens, %d categories, N=%d",
                    len(self.df), len(self.total_by_category),
                    self.total_reviews)

    def mapper(self, _, __):
        """
        Iterate over tokens in memory and yield χ² scores.
        The dummy input line is ignored; we calculated everything
        in mapper_init already.
        """
        N = self.total_reviews
        for token, cat_counts in self.df.items():
            token_total = self.total_by_token[token]

            for category, A in cat_counts.items():
                B = token_total - A
                C = self.total_by_category[category] - A
                D = N - (A + B + C)

                denom = (A + C) * (B + D) * (A + B) * (C + D)
                chi2 = ((A * D - B * C) ** 2 * N) / denom if denom else 0.0

                yield category, (token, chi2)

    # combiner / reducer helpers 
    @staticmethod
    def _topk(iterable, k):
        #Return k largest items by score
        return heapq.nlargest(k, iterable, key=lambda x: x[1])

    def combiner(self, category, token_chi_iter):
        #Local TOP_K aggregation
        for token, chi in self._topk(token_chi_iter, self.TOP_K):
            yield category, (token, chi)

    def reducer(self, category, token_chi_iter):
        #Global TOP_K per category + forward tokens for dictionary line
        top_k = self._topk(token_chi_iter, self.TOP_K)

        # Emit tokens for the merged dictionary
        for token, _ in top_k:
            yield '___TOKENS___', token

        # Build formatted line: '<category> token1:score1 token2:score2 ...'
        formatted = '\t'.join(f'{tok}:{chi:.6f}' for tok, chi in top_k)
        yield category, formatted

    #final reducer
    def merge_terms_reducer(self, key, values):
        # pass through category lines unchanged
        #for the special key, merge & sort all unique tokens alphabetically

        if key != '___TOKENS___':
            for v in values:
                yield key, v
            return

        unique_terms = sorted(set(values))
        merged = '\t'.join(unique_terms)
        yield '', merged          # empty key ⇒ line starts with tokens

    # pipeline 
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.merge_terms_reducer
            )
        ]


if __name__ == '__main__':
    MRChiSquare.run()
