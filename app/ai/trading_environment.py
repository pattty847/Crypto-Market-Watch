import gym
from gym import spaces
import numpy as np

class TradingEnvironment(gym.Env):
    def __init__(self, dataframe):
        super(TradingEnvironment, self).__init__()

        self.dataframe = dataframe
        self.reward_range = (-np.inf, np.inf)
        
        # Actions: buy=0, sell=1, hold=2
        self.action_space = spaces.Discrete(3)
        
        # multi-dimensional continuous space representing the price of the asset and indicators
        self.observation_space = spaces.Box(low=-np.inf, high=np.inf, shape=[len(dataframe.columns) + 5,])

        # initial balance
        self.asset_balance = 10000  
        self.asset_held = 0
        self.current_step = 0
        
        self.transaction_fee = 0.0075 # 0.1% fee per trade

    def step(self, action):
        current_price = self.dataframe['closes'].iloc[self.current_step]
        next_price = self.dataframe['closes'].iloc[self.current_step + 1]

        if action == 0:  # Buy
            buy_amount = self.asset_balance / current_price
            self.asset_held += buy_amount * (1 - self.transaction_fee)
            self.asset_balance = 0
        elif action == 1:  # Sell
            sell_amount = self.asset_held * current_price
            self.asset_balance += sell_amount * (1 - self.transaction_fee)
            self.asset_held = 0

        self.current_step += 1

        reward = self.asset_balance + self.asset_held * next_price
        if action == 2:
            reward -= 0.01
        
        done = self.current_step == len(self.dataframe) - 2
        info = {}

        return next_price, reward, done, info

    def reset(self):
        self.asset_balance = 10000
        self.asset_held = 0
        self.current_step = 0

        return self.get_state()
    
    def get_state(self):
        start = max(0, self.current_step - 5)
        return np.array(self.dataframe.iloc[start:self.current_step+1])