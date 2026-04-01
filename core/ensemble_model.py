import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
import os

MODEL_XGB = "xgb_model.json"
MODEL_RF = "rf_model.pkl"

xgb_model = None
rf_model = None

# =============================
# FEATURES
# =============================
def prepare_features(df):

    df = df.copy()

    df['trend'] = (df['EMA20'] > df['EMA50']).astype(int)
    df['momentum'] = (df['RSI'] > 55).astype(int)
    df['volume_spike'] = (df['Volume'] > df['Vol_MA']).astype(int)

    df['ema_spread'] = (df['EMA20'] - df['EMA50']) / df['Close']
    df['rsi_dist'] = (df['RSI'] - 50) / 50

    df['ret1'] = df['Close'].pct_change(1)
    df['ret3'] = df['Close'].pct_change(3)

    df['volatility'] = df['ATR'] / df['Close']
    df['vol_ratio'] = df['Volume'] / df['Vol_MA']

    return df.fillna(0)[[
        'trend','momentum','volume_spike',
        'ema_spread','rsi_dist','ret1','ret3',
        'volatility','vol_ratio'
    ]]

# =============================
# LOAD
# =============================
def load_models():
    global xgb_model, rf_model

    if os.path.exists(MODEL_XGB):
        xgb_model = xgb.XGBClassifier()
        xgb_model.load_model(MODEL_XGB)

    if os.path.exists(MODEL_RF):
        import joblib
        rf_model = joblib.load(MODEL_RF)

# =============================
# TRAIN
# =============================
def train_models(df):

    global xgb_model, rf_model

    df = df.copy()
    df['future'] = df['Close'].shift(-3)
    df['target'] = (df['future'] > df['Close']).astype(int)
    df = df.dropna()

    if len(df) < 100:
        return

    X = prepare_features(df)
    y = df['target']

    xgb_model = xgb.XGBClassifier(n_estimators=80, max_depth=4)
    xgb_model.fit(X, y)
    xgb_model.save_model(MODEL_XGB)

    rf_model = RandomForestClassifier(n_estimators=80)
    rf_model.fit(X, y)

    import joblib
    joblib.dump(rf_model, MODEL_RF)

# =============================
# PREDICT
# =============================
def predict(row):

    if xgb_model is None or rf_model is None:
        return 0.5

    df = pd.DataFrame([row])
    X = prepare_features(df)

    p1 = xgb_model.predict_proba(X)[0][1]
    p2 = rf_model.predict_proba(X)[0][1]

    return (p1 + p2) / 2