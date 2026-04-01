import numpy as np
import random
import pickle
import os

Q_PATH = "q_table.pkl"

class RLAgent:

    def __init__(self):
        self.q = {}
        self.epsilon = 0.2
        self.alpha = 0.1
        self.gamma = 0.9

        if os.path.exists(Q_PATH):
            with open(Q_PATH, "rb") as f:
                self.q = pickle.load(f)

    def get_state(self, regime, performance):

        perf_bucket = 1 if performance > 0 else -1
        return (regime, perf_bucket)

    def choose_action(self, state):

        if random.random() < self.epsilon:
            return random.choice([0,1,2])

        return max(self.q.get(state, {0:0,1:0,2:0}), key=self.q.get(state, {0:0,1:0,2:0}).get)

    def update(self, state, action, reward, next_state):

        self.q.setdefault(state, {0:0,1:0,2:0})
        self.q.setdefault(next_state, {0:0,1:0,2:0})

        best_next = max(self.q[next_state].values())

        self.q[state][action] += self.alpha * (
            reward + self.gamma * best_next - self.q[state][action]
        )

        with open(Q_PATH, "wb") as f:
            pickle.dump(self.q, f)