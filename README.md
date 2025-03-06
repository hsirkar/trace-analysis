# Scaling up trace analysis with Dask and Spark (i.e. scaling Pipit)

This repo contains some notebooks for using tools that are much more scalable than Pandas. These notebooks are more or less
for exploring these technologies, and whether they fit the use case for [Pipit](https://github.com/hpcgroup/pipit). If so,
they will eventually be integrated natively into Pipit to enable scalable analysis of traces.

## Design tradeoffs

There is already a tool, called [Modin](https://github.com/modin-project/modin), that attempts to use various compute backends while
exposing a Pandas DataFrame-like frontend. In fact, even Spark has this support (called the Dataframe API).

One important consideration is that these are highly optimized for column-wise operations, using techniques like
vectorization (SIMD), columnar compression, etc. In fact, all OLAP-based systems are optimized for columnar operations.

Unfortunately, many [interesting algorithms](http://www.cs.umd.edu/~bhatele/pubs/pdf/2016/tpds2016.pdf) require a row-by-row
traversal, which in the case of distributed OLAP systems, involves materializing a certain subset of rows, an expensive operation.

One simple tradeoff we can make for maximal performance is redundancy. If we store data both row-wise and column-wise,
we can perform both column-based operations, for time-series based analyses, as well as row-based operations, like the lateness
algorithm linked above.

## More links

Pipit repository: https://github.com/hpcgroup/pipit

Pipit paper: https://github.com/hsirkar/pdfs/blob/main/pipit-scholarly-paper.pdf

SC23 poster: https://github.com/hsirkar/pdfs/blob/main/sc23-pipit-poster.pptx.pdf
