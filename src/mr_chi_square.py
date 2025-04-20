from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
import sys

class MRChiSquare(MRJob):

    #overwrite the parent configure_args
    def configure_args(self):
        super(MRChiSquare, self).configure_args()
        self.add_file_arg('--dfcounts')

    # this loads all counts which will later be used for calculating ABCDN
    def load_df_counts(self):
        #count of per-term per-category
        self.df = defaultdict(lambda: defaultdict(int))
        # count of category
        self.total_by_category = defaultdict(int)
        # count of token
        self.total_by_token = defaultdict(int)
        # total count
        self.total_reviews = 0

        # was used for testing before
        dfcounts_path = self.options.dfcounts
        if hasattr(dfcounts_path, 'name'):
            dfcounts_path = dfcounts_path.name
        
        try:
            with open(dfcounts_path, 'r', encoding="utf-8") as f:
                #iterates ove lines in f
                for line in f:
                    # this is because there were some weird special "BOM" characters that were messing up everything
                    if line.startswith('\ufeff'):
                        line = line.replace('\ufeff', '')  # strip BOM if present
                    try:
                        # extracts key and value from each line
                        key, value = line.strip().split('\t')
                        # formatting
                        key = eval(key)
                        count = int(value)
                        token, category = key
                        # figures out which count to increment
                        if token == '__TOTAL__':
                            self.total_by_category[category] += count
                        elif category == '__TOTAL__':
                            self.total_by_token[token] += count
                        else:
                            self.df[token][category] = count
                    except Exception as e:
                        print("Error parsing dfcounts line:", line)
                        print("Exception:", e)

        except Exception as outer_e:
            print("Could not open dfcounts file:", outer_e)
        # sum of appearances of all categories == total count
        self.total_reviews = sum(self.total_by_category.values())

    def mapper_init(self):
        self.load_df_counts()

    # calculates ABCDN and then the chi2 values
    def mapper(self, _, line):
        for token in self.df:
            A_by_cat = self.df[token]
            token_total = self.total_by_token[token]

            for category in A_by_cat:
                A = A_by_cat[category]
                B = token_total - A
                C = self.total_by_category[category] - A
                D = self.total_reviews - (A + B + C)

                N = self.total_reviews
                num = (A * D - B * C) ** 2 * N
                denom = (A + C) * (B + D) * (A + B) * (C + D)
                chi2 = num / denom if denom != 0 else 0.0

                yield category, (token, chi2)

    # chooses top 75 for each category and sorts descending
    def combiner(self, category, token_chi_iter):
        # local top 75
        top75 = sorted(token_chi_iter, key=lambda x: -x[1])[:75]
        for token, chi in top75:
            yield category, (token, chi)

    def reducer(self, category, token_chi_iter):
        top75 = sorted(token_chi_iter, key=lambda x: -x[1])[:75]
        # collects tokens for final merged line
        for token, _ in top75:
            yield "__TOKENS__", token
        # output line for category
        output_line = "".join(f"{token}:{chi:.6f} " for token, chi in top75)
        yield category, output_line

    def reducer_final(self, key, values):
        # pass through all category outputs unmodified
        # this is for category lines
        if key != "__TOKENS__":
            for v in values:
                yield key, v
        # this is for the last line : just all the tokens
        else:
            all_terms = sorted(set(values))
            merged = " ".join(all_terms)
            yield "", merged

    # this is just combinig all steps together
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_final
            )
        ]

if __name__ == "__main__":
    MRChiSquare.run()