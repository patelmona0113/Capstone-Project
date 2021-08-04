import numpy as np

import rpy2.robjects as rb
from rpy2.robjects import numpy2ri
from rpy2.robjects.packages import importr

r = rb.r
numpy2ri.activate()


class Model(object):

    def __init__(self):
        self.model = None

    def load(self, path):
        model_rds_path = "{}.rds".format(path)
        model_dep_path = "{}.dep".format(path)

        self.model = r.readRDS(model_rds_path)

        with open(model_dep_path, "rt") as f:
            model_dep_list = [importr(dep.strip())
                              for dep in f.readlines()
                              if dep.strip() != '']

        return self

    def predict(self, X):

        if self.model is None:
            raise Exception("No Model")

        if type(X) is not np.ndarray:
            X = np.array(X)

        pred = r.predict(self.model, X)

        return int(pred)
