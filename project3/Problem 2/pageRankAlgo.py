'''

'''
import re
from operator import add
from typing import Iterable, Tuple

from pyspark.resultiterable import ResultIterable
from pyspark.sql import SparkSession


def computeContribs(urls: ResultIterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls: str) -> Tuple[str, str]:
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PageRank")\
        .getOrCreate()

    # input file format:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read\
                .text("/home/twobeers/Desktop/bigData/project3/soc-LiveJournal1.txt")\
                .rdd.map(lambda r: r[0])

#read all distinct values
#table R0:  assign to each a rank of "1"
#table R50: do iterative algorithm 50 times

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    #change to 50 for 50 iterations
    for iteration in range(1):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)


#show top 100 nodes of R50
    topHundred = [[0 for col in range(2)] for row in range(100)]

    for (link, rank) in ranks.collect():
        for i in topHundred:
            if rank > i[1]:
                i[0] = link
                i[1] = rank
                break
    
    for i in topHundred:
        print("node: " + str(i[0]) + " rank: " + str(i[1]))

    spark.stop()