import operator
import os.path

import yaml


def save_transactions(broker, txs):
    with open(os.path.join("data", broker + "-transactions.yaml"), "w") as f:
        f.write(yaml.dump(txs))


def load_transactions(broker):
    with open(os.path.join("data", broker + "-transactions.yaml"), "r") as f:
        return sorted(yaml.load(f.read()), key=operator.attrgetter("date"))
