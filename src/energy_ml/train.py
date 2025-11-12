from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from energy_ml.energy import measure_energy


def train():
    X, y = load_iris(return_X_y=True, as_frame=True)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42)
    with measure_energy("rf-train"):
        model = RandomForestClassifier(n_estimators=200, random_state=42).fit(Xtr, ytr)
    preds = model.predict(Xte)
    print("accuracy:", accuracy_score(yte, preds))


if __name__ == "__main__":
    train()
