# PART 3: Applied ML

## Task 1 sklearn

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

local_file = "file:/tmp/iris.csv"
df = pd.read_csv(local_file,\
    names = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"])

# Separate features from class.
array = df.values
X = array[:,0:4]
y = array[:,4]

# Fit Logistic Regression classifier.
logreg = LogisticRegression(C=1e5)
logreg.fit(X, y)

# Predict on training data. Seems to work.
# 5.1     3.5     1.4     0.2     Iris-setosa
# 6.2     3.4     5.4     2.3     Iris-virginica
print(logreg.predict([[5.1, 3.5, 1.4, 0.2]]))
print(logreg.predict([[6.2, 3.4, 5.4, 2.3]]))