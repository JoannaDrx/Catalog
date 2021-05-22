import sys
import pickle


def make_pickle(obj: object, path: str, verbose: bool = False) -> None:
    """
    Pickle `obj` to the specified `path`
    :param obj: A pickable object
    :param path: A local path that can be written to
    :param verbose:
    :return: None
    """
    if verbose:
        print(f'Pickling {path}')
    with open(path, 'wb') as pickle_f:
        pickle.dump(obj, pickle_f)
    if verbose:
        print(f'Done pickling to {path}')
    sys.stdout.flush()
    return


def unpickle(path: str, verbose: bool = False) -> object:
    """
    Unpickle a pickle!
    :param path: Local path to pickled object
    :param verbose:
    :return: unpickled object
    """
    if verbose:
        print(f'Unpickling {path}')
    with open(path, 'rb') as pickled_obj:
        obj = pickle.load(pickled_obj)
    if verbose:
        print(f'Done unpickling {path}')
    sys.stdout.flush()
    return obj
