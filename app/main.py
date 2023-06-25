import asyncio
import logging
import ccxt

from api.influx import InfluxDB
from api.watch_trades import Trades
from api.fetch_candles import Candles
from ai.trading_environment import TradingEnvironment
from ai.q_learning import QLearningAgent
from ui.chart import Chart
from ui.main_menu import MainMenu
from api.ccxt_interface import CCXTInterface
from ui.viewport import View_Port
from api.server import Server

    
async def main():
    async with Candles(local_database=True) as manager:
        await manager.load_exchanges(['kucoinfutures'])
        
        candles = await manager.fetch_candles([{"exchange":"kucoinfutures", "symbol":"BTC/USDT:USDT", "timeframe":"1h"}], None, 1000)
        
        for exchange_id, symbol, timeframe, dataframe in candles:
            print(f"Exchange ID: {exchange_id}, Symbol: {symbol}, Timeframe: {timeframe}")
            
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
            
                logging.info(f"Episode {i+1}/{n_episodes} complete, balance: {env.asset_balance}")
            
        # def train(agent, env):
        # Initialize the agent and the environment
        # Loop over episodes
            # Loop over the steps in each episode
                # At each step, have the agent choose an action and then take that action in the environment
                # Have the agent learn from the results of that action

if __name__ == "__main__":
    asyncio.run(main())