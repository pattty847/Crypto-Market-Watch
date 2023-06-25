import random

class QLearningAgent:
    def __init__(self, action_space):
        self.action_space = action_space
        self.q_table = {}

    def get_action(self, state, epsilon=0.1):
        if random.uniform(0, 1) < epsilon:
            return self.action_space.sample()  # Explore action space
        else:
            if state not in self.q_table or all(value == 0 for value in self.q_table[state].values()):
                return self.action_space.sample()  # Explore action space
            else:
                return max(self.q_table[state], key=self.q_table[state].get)  # Exploit learned values

    def update_q_table(self, state, action, reward, next_state, alpha=0.5, gamma=0.95):
        old_value = self.q_table[state][action] if state in self.q_table and action in self.q_table[state] else 0
        next_max = max(self.q_table[next_state].values()) if next_state in self.q_table else 0

        new_value = (1 - alpha) * old_value + alpha * (reward + gamma * next_max)

        if state not in self.q_table:
            self.q_table[state] = {}
        self.q_table[state][action] = new_value