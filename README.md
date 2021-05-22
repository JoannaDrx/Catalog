# Catalog
A lightweight bookkeeping module that facilitates record-keeping and file tracking for data science projects at Second
Genome. Catalog is built on the central assumption that every Jira issue ID is traced back to a script or jupyter 
notebook in a repository, as well as to a folder in an S3 bucket containing all related output files.
This module facilitates issue tracking, allows to not copy/paste s3 paths ad nauseum, and provides long term inventory
for a particular project that can be easily re-consulted down the line.

Quick start
```python
from Catalog.main import Catalog, DataSet

cat = Catalog(s3_basepath)
```

# show catalog contents here

# show easy upload using DataSet here

# show search fucntion

# show array capability.
