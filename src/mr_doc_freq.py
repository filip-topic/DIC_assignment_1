from mrjob.job import MRJob
import re
import json
import logging
from timeit import default_timer as timer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TOKEN_RE = re.compile(r"[\s\t\d()\[\]{}.!?,;:+=\-_'\"`~#@&*%€$§\\/]+")

# converts to lowercase, splits by delimeter regex and filters out stopwords and tokens of length 1
def tokenize(text, STOPWORDS):
    tokens = TOKEN_RE.split(text.lower())
    return [t for t in tokens if t not in STOPWORDS and len(t) > 1]

# MapReduce job that calculates how many times each token appearns in each category
class MRDocFreq(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords', default='stopwords.txt')

    def mapper_init(self):
        # load stopwords once when the mapper starts
        with open(self.options.stopwords, 'r', encoding='utf-8') as f:
            self.stopwords = set(f.read().split())

    #read each line in the data
    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get('category', None)
            review_text = review.get('reviewText', '')

            if category is None:
                print("Missing category:", review)
            else:
                # represent tokens as a set - to avoid duplication of tokens in a sentence
                tokens = set(tokenize(review_text, self.stopwords))
                # count the occurance of this category (1)
                yield ('__TOTAL__', category), 1
                for token in tokens:
                    # count occurance of this token (unigram) in this category (1)
                    yield (token, category), 1
                    # count the overall occurance of this token (1)
                    yield (token, '__TOTAL__'), 1
        except:
            pass

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    t1 = timer()
    MRDocFreq.run()
    t2 = timer()
    runtime = t2 - t1
    logger.info(f"time: {runtime:.2f}s")
    