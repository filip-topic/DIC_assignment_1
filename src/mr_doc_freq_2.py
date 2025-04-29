from mrjob.job import MRJob
import re
import json
import logging
from collections import defaultdict
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

    JOBCONF = {
        "mapreduce.output.fileoutputformat.compress": "true",
        "mapreduce.output.fileoutputformat.compress.codec":
            "org.apache.hadoop.io.compress.GzipCodec",
        "mapreduce.output.fileoutputformat.compress.type": "BLOCK",
        # (optional) compress intermediate map output as well:
        "mapreduce.map.output.compress": "true",
        "mapreduce.map.output.compress.codec":
            "org.apache.hadoop.io.compress.SnappyCodec",
    }

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords', default='stopwords.txt')

    def mapper_init(self):
        with open(self.options.stopwords, 'r', encoding='utf-8') as f:
            self.stopwords = set(f.read().split())

        # agg_dict[(token, category)] = count
        self.local_counts = defaultdict(int)

    #read each line in the data
    def mapper(self, _, line):
        try:
            review = json.loads(line)
            cat = review.get('category')
            if not cat:
                self.increment_counter('DocFreq', 'MissingCategory', 1)
                return

            tokens = set(tokenize(review.get('reviewText', ''), self.stopwords))
            self.local_counts[('__TOTAL__', cat)] += 1           # doc counter
            for tok in tokens:
                self.local_counts[(tok, cat)] += 1                # A
                self.local_counts[(tok, '__TOTAL__')] += 1        # token total
        except json.JSONDecodeError:
            self.increment_counter('DocFreq', 'BadJSON', 1)

    def mapper_final(self):
        for key, cnt in self.local_counts.items():
            yield key, cnt   

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
    