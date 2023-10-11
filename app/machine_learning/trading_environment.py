import numpy as np
import pandas as pd
import gym
from gym import spaces

class TradingEnvironment(gym.Env):
    def __init__(self, dataframe, rows_of_memory=5):
        super(TradingEnvironment, self).__init__()

        # Define action and observation space
        # They must be gym.spaces objects
        # (action, amount) - (0, 0.5) - buy 50% of the asset
        self.action_space = spaces.Tuple((spaces.Discrete(4), spaces.Box(low=0, high=1, shape=(1,), dtype=np.float32))) 
        
        
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=(rows_of_memory, len(dataframe[0].columns)), dtype=np.float32) 

        # Initialize state
        self.dataframe = dataframe
        self.rows_of_memory = rows_of_memory
        self.initial_portfolio_value = 10000
        self.current_step = 0
        self.current_portfolio_value = self.initial_portfolio_value
        
        self.reset()

    def step(self, action):
        # Execute one time step within the environment
        self.current_step += 1

        done = self.current_step >= len(self.dataframe)
        reward = self.get_reward()
        
        return self.get_state(), reward, done, {}

    def get_state(self, t):
        # Return the last window_size rows from the dataframes
        return self.current_portfolio_value - self.initial_portfolio_value

    def get_reward(self):
        # Calculate the reward based on the current state and portfolio
        pass

    def reset(self):
        # Reset the state of the environment to an initial state
        pass

    def render(self, mode='human'):
        # Render the environment to the screen (optional)
        pass