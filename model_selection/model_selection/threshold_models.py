import json
import numpy as np
from sklearn.base import ClassifierMixin, BaseEstimator
from sklearn.utils.validation import check_X_y, check_array, check_is_fitted
from sklearn.metrics import fbeta_score


class SingleThresholdClassifier(ClassifierMixin, BaseEstimator):
    """An example classifier which implements a single threshold model where
    it checks X against a single hyperparameter, k, to predict a binary class.
    If X >= k, then it predicts 1 (True). If X < k, then it predicts 0 (False).
    If x in X contain multiple values, then every value must be >= k to be True.

    Parameters
    ----------
    model_name : str, default='demo'
        A parameter used to name the model for convenience purposes.

    Attributes
    ----------
    X_ : ndarray, shape (n_samples, n_features)
        The input passed during :meth:`fit`.
    y_ : ndarray, shape (n_samples,)
        The labels passed during :meth:`fit`.
    k_: integer
        The ping rate threshold of the optimal model.
    optimal_score_: float
        The optimal F0.5 score at threshold k_.
    test_thresholds_: array-like
        The thresholds tested during :meth:`fit`.
    threshold_scores_: array-like, shape (len(test_thresholds_), )
        The model score at each of the thresholds in test_thresholds_.

    """

    def __init__(self, model_name="no name"):
        self.model_name = model_name

    def save(self, filename):

        if not filename.endswith(".json"):
            raise ValueError("filename must be of type .json")

        save_json = {
            "X_": self.X_,
            "y_": self.y_,
            "k_": self.k_,
            "optimal_score_": self.optimal_score_,
            "test_thresholds_": self.test_thresholds_,
            "threshold_scores_": self.threshold_scores_,
        }

        with open(filename, "w") as outfile:
            json.dump(save_json, outfile)

    def load(self, filename):

        if not filename.endswith(".json"):
            raise ValueError("filename must be of type .json")

        with open(filename, "r") as outfile:
            params = json.load(outfile)
            self.X_ = params["X_"]
            self.y_ = params["y_"]
            self.k_ = params["k_"]
            self.optimal_score_ = params["optimal_score_"]
            self.test_thresholds_ = params["test_thresholds_"]
            self.threshold_scores_ = params["threshold_scores_"]

    def fit(self, X, y):
        """A reference implementation of a fitting function for a classifier.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            The training input samples.
        y : array-like, shape (n_samples,)
            The target values. An array of int.
        Returns
        -------
        self : object
            Returns self.
        """
        # Check that X and y have correct shape
        X, y = check_X_y(X, y)

        self.X_ = X.tolist()
        self.y_ = y.tolist()

        # Select the optimal threshold_ value from 1 to 60 using F0.5 score.
        test_thresholds = np.arange(1, 61)
        threshold_scores = []
        for thresh in test_thresholds:
            classes_for_thresh = np.where(
                np.where(X >= thresh, 1, 0).all(axis=1, keepdims=True), 1, 0
            )
            threshold_scores.append(
                fbeta_score(y, classes_for_thresh, beta=0.5, average="binary")
            )

        self.test_thresholds_ = test_thresholds.tolist()
        self.threshold_scores_ = threshold_scores
        
        self.optimal_score_ = max(threshold_scores)
        self.k_ = int(test_thresholds[threshold_scores.index(self.optimal_score_)])

        return self

    def predict(self, X):
        """Predict the class based on how each value x in X relates to k_.
        If x >= k_, then 1 (True). If x < k_, then 0 (False).

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            The input samples.
        Returns
        -------
        y : ndarray, shape (n_samples,)
            The label for each sample is the label of the closest sample
            seen during fit.
        """
        check_is_fitted(self, ["X_", "y_"])
        X = check_array(X)

        return np.where(
            np.where(X >= self.k_, 1, 0).all(axis=1, keepdims=True), 1, 0
        ).flatten()


class DoubleThresholdClassifier(ClassifierMixin, BaseEstimator):
    """An example classifier which implements a double threshold model where
    it checks X against two hyperparameters, k and j, to predict a binary class.
    For each x in X, if the ping rate variable is >= k AND the reception variable
    is >= j, then it predicts 1 (True). Otherwise, it predicts 0 (False).
    If there are multiple ping rate variables, then all of them must be >= k to be True.

    Parameters
    ----------
    model_name : str, default='demo'
        A parameter used to name the model for convenience purposes.

    Attributes
    ----------
    X_ : ndarray, shape (n_samples, n_features)
        The input passed during :meth:`fit`.
        Reception values must be in index 0 for each data point.
    y_ : ndarray, shape (n_samples,)
        The labels passed during :meth:`fit`.
    k_: integer
        The ping rate threshold of the optimal model.
    j_: integer
        The reception threshold of the optimal model.
    optimal_score_: float
        The optimal F0.5 score at thresholds k_ and j_.
    test_thresholds_pings_: array-like
        The thresholds tested during :meth:`fit`.
    test_thresholds_rec_: array-like
        The thresholds tested during :meth:`fit`.
    threshold_scores_: array-like, shape (len(test_threshold_pings_), len(test_thresholds_rec_))
        The model score at each of the thresholds in test_thresholds_.

    """

    def __init__(self, model_name="no name"):
        self.model_name = model_name

    def save(self, filename):

        if not filename.endswith(".json"):
            raise ValueError("filename must be of type .json")

        save_json = {
            "X_": self.X_,
            "y_": self.y_,
            "k_": self.k_,
            "j_": self.j_,
            "optimal_score_": self.optimal_score_,
            "test_thresholds_pings_": self.test_thresholds_pings_,
            "test_thresholds_rec_": self.test_thresholds_rec_,
            "threshold_scores_": self.threshold_scores_,
        }

        with open(filename, "w") as outfile:
            json.dump(save_json, outfile)

    def load(self, filename):

        if not filename.endswith(".json"):
            raise ValueError("filename must be of type .json")

        with open(filename, "r") as outfile:
            params = json.load(outfile)
            self.X_ = params["X_"]
            self.y_ = params["y_"]
            self.k_ = params["k_"]
            self.j_ = params["j_"]
            self.optimal_score_ = params["optimal_score_"]
            self.test_thresholds_pings_ = params["test_thresholds_pings_"]
            self.test_thresholds_rec_ = params["test_thresholds_rec_"]
            self.threshold_scores_ = params["threshold_scores_"]

    def fit(self, X, y):
        """A reference implementation of a fitting function for a classifier.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            The training input samples.
            Reception values must be in index 0 for each data point.
        y : array-like, shape (n_samples,)
            The target values. An array of int.
        Returns
        -------
        self : object
            Returns self.
        """
        X, y = check_X_y(X, y)

        self.X_ = X.tolist()
        self.y_ = y.tolist()

        X_rec = np.apply_along_axis(lambda x: x[:1], 1, X)
        X_pings = np.apply_along_axis(lambda x: x[1:], 1, X)

        test_thresholds_rec = np.arange(6, 61)
        test_thresholds_pings = np.arange(1, 61)

        # Select the optimal threshold_ value from 1 to 60 using F0.5 score.
        threshold_scores = []
        for rec_thresh in test_thresholds_rec:

            # Test reception for each data point against threshold
            rec_thresh_test = np.where(X_rec >= rec_thresh, 1, 0).flatten()

            threshold_scores_for_rec = []
            for ping_thresh in test_thresholds_pings:

                # Test ping rates for each data point against threshold
                pings_thresh_test = np.where(
                    np.where(X_pings >= ping_thresh, 1, 0).all(axis=1, keepdims=False),
                    1,
                    0,
                )

                # Set class based on if each data point passes both tests or not
                classes_for_thresh = np.where(
                    np.asarray(list(zip(rec_thresh_test, pings_thresh_test))).all(
                        axis=1, keepdims=True
                    ),
                    1,
                    0,
                )

                # Calculate the F0.5 score
                threshold_scores_for_rec.append(
                    fbeta_score(y, classes_for_thresh, beta=0.5, average="binary")
                )

            threshold_scores.append(threshold_scores_for_rec)

        self.test_thresholds_pings_ = test_thresholds_pings.tolist()
        self.test_thresholds_rec_ = test_thresholds_rec.tolist()
        self.threshold_scores_ = threshold_scores
        
        self.optimal_score_ = max([max(l) for l in threshold_scores])
        self.j_ = int(
            test_thresholds_rec[
                [max(l) for l in threshold_scores].index(self.optimal_score_)
            ]
        )
        max_model = threshold_scores[self.j_ - 1]
        self.k_ = int(test_thresholds_pings[max_model.index(max(max_model))])

        return self

    def predict(self, X):
        """Predict the class based on how each x in X relates to k_ and j_.
        For each x in X, if the ping rate variable is >= k AND the reception variable
        is >= j, then it predicts 1 (True). Otherwise, it predicts 0 (False).
        If there are multiple ping rate variables, then all of them must be >= k to be True.

        Parameters
        ----------
        X : array-like, shape (n_samples, n_features)
            The input samples.
            Reception values must be in index 0 for each data point.
        Returns
        -------
        y : ndarray, shape (n_samples,)
            The label for each sample is the label of the closest sample
            seen during fit.
        """
        check_is_fitted(self, ["X_", "y_"])
        X = check_array(X)

        # Test reception for each data point against threshold
        X_rec = np.apply_along_axis(lambda x: x[:1], 1, X)
        rec_thresh_test = np.where(X_rec >= self.j_, 1, 0).flatten()

        # Test ping rates for each data point against threshold
        X_pings = np.apply_along_axis(lambda x: x[1:], 1, X)
        pings_thresh_test = np.where(
            np.where(X_pings >= self.k_, 1, 0).all(axis=1, keepdims=False), 1, 0
        )

        # Set class based on if each data point passes both tests or not
        predicted_classes = np.where(
            np.asarray(list(zip(rec_thresh_test, pings_thresh_test))).all(
                axis=1, keepdims=True
            ),
            1,
            0,
        )

        return predicted_classes.flatten()
