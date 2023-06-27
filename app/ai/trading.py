import logging

import pandas as pd

from .rl_test import StockTradingEnv
from .q_learning_agent import QLearningAgent
from .trading_environment import TradingEnvironment
from stable_baselines3.common.vec_env import DummyVecEnv
from stable_baselines3 import PPO


class Trading:
    def run(self, dataframe: pd.DataFrame):
        env = TradingEnvironment(dataframe)
        agent = QLearningAgent(env.action_space)
        
        n_episodes = 500
        
        for i in range(n_episodes):
            state = env.reset()
            
            for t in range(len(dataframe) - 1):
                action = agent.get_action(state)
                
                next_state, reward, done, _ = env.step(action)
                
                agent.update_q_table(state, action, reward, next_state)
                
                state = next_state
                
                if done:
                    break
        
            logging.info(f"Episode {i+1}/{n_episodes} complete, balance: {env.asset_balance}, asset_held: {env.asset_held}")
            
    def run_test(self, dataframe: pd.DataFrame):
        # The algorithms require a vectorized environment to run
        env = DummyVecEnv([lambda: StockTradingEnv(dataframe)])

        model = PPO('MlpPolicy', env, verbose=1)
        model.learn(total_timesteps=20000)

        obs = env.reset()
        for i in range(2000):
            action, _states = model.predict(obs)
            obs, rewards, done, info = env.step(action)
            env.render()