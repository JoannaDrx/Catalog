import os
from dataclasses import dataclass, field
import pandas as pd
import s3
from utils import unpickle, make_pickle


def format_jira(jid: str) -> str:
    """
    Transforms jira ID to keep same notation across the project.
    :param jid: Jira ID (ex: SGDS-123, or OMICS-456_do_something)
    """
    jid = jid.lower().strip()
    jid = jid.split('_')[0]
    j = "".join(jid.split('-'))
    return j


def _build_s3_path(s3_basepath: str, jira_issue: str, loc_path: str, subfolder: str = None) -> str:
    """
    Build an s3 path with optional subfolder
    :param s3_basepath: Target dir on S3
    :param jira_issue: Unformatted jira ID
    :param loc_path: Local path of file
    :param subfolder: Name of optional subfolder
    """
    return os.path.join(s3_basepath, format_jira(jira_issue), subfolder, os.path.basename(loc_path))


@dataclass
class Catalog:
    """
    Catalog maps jira issue IDs to s3 outputs to facilitate project management. A catalog belongs to
     a specific repository, project, & S3 location. It is pickled and saved to its repository.
    :param cat_path: path to the pickled catalog in the project repo
    :param s3_basepath: S3 prefix path to the project files
    :param contents: dict of DataSet objects
    :param fresh: If True, create a new Catalog from scratch, if False, read in the pickle from cat_path
    """

    # get repo paths
    cat_path: str
    s3_basepath: str
    contents: dict = field(init=False)
    fresh: bool = False
    verbose: bool = False

    def __post_init__(self):

        if self.fresh:
            self.create()

        else:
            try:
                print(f"Unpickling {self.cat_path}. Exists? {os.path.exists(self.cat_path)}")
                self.contents = unpickle(self.cat_path, verbose=self.verbose)
            except Exception as e:
                print(f'ERROR Reading in Catalog pickle from {self.cat_path}. {e}.'
                      f'Generating empty catalog.')
                self.contents = {}
            if self.verbose:
                print(f'Initialized Catalog with {len(self.contents)} records.')

    def __repr__(self):
        return f'Catalog for {self.s3_basepath}: {len(self.contents)} records.'

    def create(self):
        """
        Create a new catalog from scratch. Crawls the s3_basepath to collect all DataSet objects.
        """
        self.contents = {}
        for jira_path in s3.ls(self.s3_basepath):
            jira_issue = format_jira(jira_path.split('/')[-2])
            if self.verbose:
                print(f'Creating JIRA issue {jira_issue}...')
            self.contents[jira_issue] = {}
            self._update(s3.ls(jira_path), jira_issue)
        self._save()

    def update(self, jira_issue: str = None, format_jid: bool = True, arrays: bool = False) -> None:
        """
        Crawls S3 to update catalog file.
        :param jira_issue: specfic Jira issue to update. If None, look for Jira IDs that exist on S3
                           but are not present in the Catalog.
        :param format_jid: whether to format `jira_issue`. Useful for files/paths not following
                           Catalog nomenclature
        :param arrays: whether to create DataSet arrays or force individual files
        """

        if jira_issue:
            jira_issue = format_jira(jira_issue) if format_jid else jira_issue

            # Look for the s3 path corresponding to this jira item
            jira_path = s3.ls(self.s3_basepath, pattern=jira_issue)
            if len(jira_path) != 1:
                raise ValueError(f"Found {len(jira_path)} prefixes matching {jira_issue}.")
            else:
                jira_path = jira_path[0]

            s3_list = s3.ls(jira_path)
            self.contents[jira_issue] = {}
            self._update(s3_list, jira_issue, arrays=arrays)

        else:  # update all not yet in Catalog
            print('Scanning for new records...')
            for jira_path in s3.ls(self.s3_basepath):
                jira_issue = format_jira(jira_path.split('/')[-2])
                if jira_issue not in self.contents:
                    print(f'Creating JIRA issue {jira_issue}...')
                    s3_list = s3.ls(jira_path)
                    self.contents[jira_issue] = {}
                    self._update(s3_list, jira_issue, arrays=arrays)

        self._save()

    def search(self, **kwargs: dict) -> list:
        results = []
        for jira, cont in self.contents.items():
            results.extend([i for i in cont if all(kwargs[k] in i[k] for k in kwargs)])
        if results:
            print(f'Found {len(results)} results for search: {str(kwargs)}')
        return results

    def _save(self) -> None:
        make_pickle(self.contents, self.cat_path, verbose=self.verbose)
        if self.verbose:
            print(f'Catalog was saved to {self.cat_path}')

    def _update(self, s3_list: list, jira_issue: str, arrays: bool = True):
        # add any subfolder contents
        folders = [p for p in s3_list if s3.is_prefix(p)]
        for prefix in folders:
            self._gen_array_records(s3.ls(prefix), jira_issue)

        # add any lone files
        file_list = [p for p in s3_list if p not in folders]
        if arrays and len(file_list) > 10:
            self._gen_array_records(file_list, jira_issue)
        else:
            for fp in file_list:
                name, ext = os.path.basename(fp).rsplit('.', 1)
                dataset = DataSet(jira_issue, fp, format=ext.upper(), dtype='file')

                # Look for an existing dataset or set of datasets with this name
                if name in self.contents[jira_issue]:
                    ds = self.contents[jira_issue].pop(name)
                    if type(ds) is dict:
                        ds[ext] = dataset
                        dataset = ds
                    else:
                        dataset = {
                            ds.format.lower(): ds,
                            ext.lower(): dataset
                        }

                # Update contents
                self.contents[jira_issue][name] = dataset

    def _gen_array_records(self, array_list, jira_issue):
        bpath = os.path.dirname(array_list[0])

        # define arrays by file extensions, skip subfolders
        ext = set([c.split('.')[-1] for c in array_list if not c.endswith('/')])

        for e in ext:

            # make a distinct array for each file format/extension, even if they share the
            # same subfolder
            if len(ext) > 1:
                name = '_'.join([os.path.basename(bpath), e.upper(), 'array'])
            else:
                name = '_'.join([os.path.basename(bpath), 'array'])

            r = [os.path.basename(rec) for rec in array_list if rec.endswith('.' + e)]
            if len(r) == 0:  # single file no extension
                meta = {'format': 'NA', 'dtype': 'file'}
                fp = e

            elif len(r) == 1:  # single file
                fp = os.path.join(bpath, r[0])
                meta = {'format': e.upper(), 'dtype': 'file'}

            else:
                # make a dummy path to show regex pattern of array files
                repr_path = os.path.join(bpath, self._gen_repr_path(r))
                fp = bpath + '/'
                meta = {
                    'count': len(r),
                    'format': e.upper(),
                    'dtype': 'array',
                    'regex': repr_path,
                    'example': os.path.join(bpath, r[0])
                }

            dataset = DataSet(jira_issue, fp, **meta)
            self.contents[jira_issue][name] = dataset

    @staticmethod
    def _gen_repr_path(arr):
        temp = arr[0]
        root = ''
        for char in temp:
            if all(a.startswith(root + char) for a in arr):
                root += char
            else:
                break
        end = ''
        for char in temp[::-1]:
            if all(v.endswith(char + end) for v in arr):
                end = char + end
            else:
                break
        return root + '*' + end


@dataclass
class DataSet:
    """
    DataSet object facilitates read/writes of analysis outputs.
    :param jira_issue: The jid this output relates to
    :param s3_path: S3 location of this output
    :param format: file format
    :param count: For arrays, number of files in array
    :param dtype: array or file
    :param regex: path regex for array DataSets
    :param example: example path for an array DataSet member
    """
    jira_issue: str
    s3_path: str
    format: str = None
    count: int = None
    dtype: str = None
    regex: str = None
    example: str = None

    def __repr__(self):
        repr = f'DataSet object from {self.jira_issue.upper()}:'
        for attr, val in self.__dict__.items():
            if val and attr != 'jira_issue':
                repr += f'\n\t- {attr}: {val}'
        return repr

    def __post_init__(self):
        self.jira_issue = format_jira(self.jira_issue)

    def _get_object_path(self, key):
        if self.dtype == 'array':  # build s3 path using key
            if not key or not self.format:
                raise ValueError('Unable to download DataSet array member without key or format.')
            return os.path.join(self.s3_path, f'{key}.{self.format.lower()}')
        return self.s3_path

    @property
    def keys(self):
        """Get existing keys for an array DataSet, which can be used in `read` and
        `download` functions to access an array member"""
        if self.dtype != 'array':
            raise TypeError('Property `keys` only exists for DataSet arrays')
        return [os.path.basename(p).split('.')[0] for p in
                s3.ls(self.s3_path, suffix=self.format.lower())]

    def download(self, key: str = None, tmp: str = '/tmp/') -> str:
        """
        Download data from S3 to temp folder, return local path
        :param key: For array DataSet objects, specifies name of array member to download.
        :param tmp: Local dir files are written to
        """
        path = self._get_object_path(key)
        return s3.copy(path, dest=tmp, verbose=False)

    def read(self, idx: (int, str) = 0, key: str = None, **kwargs) -> pd.DataFrame:
        """
        Stream data from S3. Uses pandas.read method if file is a CSV
        :param key: For array DataSet objects, specifies name of array member to stream.
        :param idx: `index_col` kwarg for pandas.read method
        """
        path = self._get_object_path(key)
        if self.format == 'CSV':
            return pd.read_csv(path, index_col=idx, **kwargs)
        return s3.read(path, **kwargs)

    @classmethod
    def from_local_file(cls, loc_path: str, jira_issue: str, s3_basepath: str,
                        subfolder: str = None):
        """Create a DataSet object from a local path"""

        s3_path = _build_s3_path(s3_basepath, jira_issue, loc_path, subfolder=subfolder)
        print(s3_path)
        s3.copy(loc_path, dest=s3_path, verbose=False)
        return cls(jira_issue, s3_path, dtype='file', format=loc_path.rsplit('.', 1)[1].upper())

    @classmethod
    def from_df(cls, dataframe: pd.DataFrame, name: str, jira_issue: str, s3_basepath: str,
                subfolder: str = None, tmp: str = '/tmp/'):
        """Create a DataSet object from a pandas data frame"""

        s3_path = _build_s3_path(s3_basepath, jira_issue, name + '.csv', subfolder=subfolder)
        print(s3_path)
        # dataframe.to_csv(s3_path)  # this fails silently when using credentials
        # (as opposed to with an instance role). Filed LUCHA-1780
        dataframe.to_csv(f'{tmp}{name}.csv')
        s3.copy(f'{tmp}{name}.csv', dest=s3_path, verbose=False)
        return cls(jira_issue, s3_path, format='CSV', dtype='file')
