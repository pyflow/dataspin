import random
import string
from datetime import datetime


class SimpleFetcher(object):
    def __init__(self, datasets, **kwargs):
        self.datasets = datasets

    def fetch(self):
        def random_name():
            return ''.join([random.choice(string.ascii_lowercase) for i in range(0, 8)])

        def random_count():
            return random.randrange(10, 1000)

        ret = {}
        for dataset in self.datasets:
            ret[dataset] = []
            for i in range(0, random.randrange(80, 100)):
                if dataset == "dataset_a":
                    ret[dataset].append({"date": str(datetime.now()), "name": random_name(), "a_count": random_count()})
                elif dataset == "dataset_b":
                    ret[dataset].append({"date": str(datetime.now()), "name": random_name(), "b_count": random_count()})
        return ret


class SimpleSource(object):
    source_type = 'simple'
    dataset_schemas = {
        "dataset_a": {
            "date": {
                "type": "datetime",
                "nullable": False
            },
            "name": {
                "type": "string"
            },
            "a_count": {
                "type": "integer"
            }
        },
        "dataset_b": {
            "date": {
                "type": "datetime",
                "nullable": False
            },
            "name": {
                "type": "string"
            },
            "b_count": {
                "type": "integer"
            }
        }
    }

    @classmethod
    def datasets(cls):
        return cls.dataset_schemas.keys()

    def fetch(self, **kwargs):
        datasets = kwargs.get('datasets', self.datasets())
        sf = SimpleFetcher(datasets, **kwargs)
        return sf.fetch()
