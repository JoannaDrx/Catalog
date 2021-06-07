# Catalog
A lightweight bookkeeping module that facilitates record-keeping and file tracking for data science projects. 
Catalog builds on the assumption that every Jira issue ID is traced back to a script or jupyter notebook in a 
repository, as well as to a folder in an S3 bucket containing all related output files.
This module facilitates issue tracking, avoids copy/pasting of s3 paths, and provides a long term inventory for a 
project, enabling reproducibility.

## Quick start
Add the following snippet to your project's `__init__.py` file:
```python
import pathlib
import Catalog

# Project catalog
s3_basepath = 's3://my-bucket/my-project-folder/'

cat_path = pathlib.Path(__file__).parent.resolve().joinpath('catalog.pkl')
if not cat_path.exists():
    catalog = Catalog(cat_path, s3_basepath, fresh=True)
else:
    catalog = Catalog(cat_path, s3_basepath)
```

Then from a jupyter notebook or script, you can simply call your project's Catalog with:
```python
from my_project import catalog as cat
cat
Out[1]: Catalog for s3://test-bucket/my-project: 3 records.
```

#### Accessing contents
```python
# list all records in catalog
cat.contents.keys()
Out[2]: dict_keys(['jid123', 'jid456', 'jid789'])

# access a specific record
cat.contents['jid123'].keys()
Out[3] : dict_keys(['master_results', 'job_configs'])

# access details on a file belonging to a record
cat.contents['jid123']['master_results']
Out[4]: DataSet object from SGDS157:
            - s3_path: s3://test-bucket/my-project/jid123/results_batch20210315.csv
            - format: CSV
            - dtype: file
```
### Searching Catalog
```python
# search can be performed by name, jid, format, dtype or path stub.
cat.search(format='JSON', dtype='array')
Out[6]: Found 2 results for query {'format': 'JSON', 'dtype': 'array'}:
- jid123: {job_configs:
DataSet object from jid123:
    - s3_path: s3://test-bucket/my-project/jid123/configs/
    - format: JSON
    - dtype: array
    - example: s3://test-bucket/my-project/jid123/configs/0abehs3vd4556_cfg.json
    - regex: s3://test-bucket/my-project/jid123/configs/*_cfg.json
    - count: 26}

- jid456: {results_metrics:
DataSet object from jid456:
    - s3_path: s3://test-bucket/my-project/jid456/metrics/
    - format: JSON
    - dtype: array
    - example: s3://test-bucket/my-project/jid456/metrics/0abehs3vd4556.json
    - regex: s3://test-bucket/my-project/jid456/metrics/*.json
    - count: 26}

```

### Updating Catalog
```python
cat.udpate()  # walks the s3_basepath and adds any JID not present in dictionary
cat.udpate('jid-123')  # specifically updates requested JID
```


## DataSet objects
DataSet objects are entries in a Catalog. They can be created from a pandas object with:
```python
DataSet.from_df(df, 'selected_features', 'JID-456', s3_basepath)
Out[8]: s3://test-bucket/my-project/jid456/selected_features.csv
DataSet(jira_issue='jid456', 
        s3_path='s3://test-bucket/my-project/jid456/selected_features.csv', 
        format='CSV', count=None, dtype='file', regex=None, example=None, 
        verbose=False)
```

And loaded into memory with
```python
# if file is a CSV - streams contents directly and returns a pandas DataFrame object
df = cat.contents['jid123']['master_results'].read()

# otherwise - returns the local path of the downloaded object
loc_path = cat.contents['jid123']['master_results'].download(tmp='/my/tmp/dir/')
```

You can also create array DataSet objects which represent a set of files:
```python
cat.contents['jid123']['job_configs']
Out[10]:
DataSet object from jid123:
    - s3_path: s3://test-bucket/my-project/jid123/configs/
    - format: JSON
    - dtype: array
    - example: s3://test-bucket/my-project/jid123/configs/0abehs3vd4556_cfg.csv
    - regex: s3://test-bucket/my-project/jid123/configs/*_cfg.json
    - count: 26
```