import random

class QLearningAgent:
    def __init__(self, action_space):
        # Initialize the agent with the available actions it can take
        self.action_space = action_space
        # Initialize an empty Q-table
        self.q_table = {}

    def get_action(self, state, epsilon=0.1):
        # With probability epsilon, we should explore the action space
        if random.uniform(0, 1) < epsilon:
            return self.action_space.sample()  # Explore action space
        else:
            state = tuple(state.flatten())
            # If the state is not in the Q-table or all its values are 0, explore
            if state not in self.q_table or all(value == 0 for value in self.q_table[state].values()):
                return self.action_space.sample()  # Explore action space
            else:
                # If the state is in the Q-table, exploit by choosing the action with the highest Q-value
                return max(self.q_table[state], key=self.q_table[state].get)  # Exploit learned values

    def update_q_table(self, state, action, reward, next_state, alpha=0.5, gamma=0.95):
        # convert state and next_state to tuples
        state = tuple(state.flatten())
        next_state = tuple(next_state.flatten())

        # Get the old Q-value for the state-action pair
        old_value = self.q_table[state][action] if state in self.q_table and action in self.q_table[state] else 0
        # Get the maximum Q-value for the next state
        next_max = max(self.q_table[next_state].values()) if next_state in self.q_table else 0

        # Calculate the new Q-value for the state-action pair
        new_value = (1 - alpha) * old_value + alpha * (reward + gamma * next_max)

        # Update the Q-table with the new Q-value
        if state not in self.q_table:
            self.q_table[state] = {}
        self.q_table[state][action] = new_value