import gym
from gym import spaces
import numpy as np

class TradingEnvironment(gym.Env):
    def __init__(self, df):
        super(TradingEnvironment, self).__init__()

        self.df = df
        self.reward_range = (-np.inf, np.inf)
        self.action_space = spaces.Discrete(3)
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=[1,])

        self.asset_balance = 10000  # initial balance
        self.asset_held = 0
        self.current_step = 0

    def step(self, action):
        current_price = self.df['closes'].iloc[self.current_step]
        next_price = self.df['closes'].iloc[self.current_step + 1]

        if action == 0:  # Buy
            self.asset_held += self.asset_balance / current_price
            self.asset_balance = 0
        elif action == 1:  # Sell
            self.asset_balance += self.asset_held * current_price
            self.asset_held = 0

        self.current_step += 1

        reward = self.asset_balance + self.asset_held * next_price
        done = self.current_step == len(self.df) - 2
        info = {}

        return next_price, reward, done, info

    def reset(self):
        self.asset_balance = 10000
        self.asset_held = 0
        self.current_step = 0

        return self.df['closes'].iloc[self.current_step]
