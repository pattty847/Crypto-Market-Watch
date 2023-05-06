# Crypto-Market-Watch
This Python application is a real-time market aggregator that collects and processes trade data from multiple cryptocurrency exchanges using the CCXT Pro library. It organizes trade data based on the traded volume and Cumulative Volume Delta (CVD) for various order size categories. The aggregated data is then displayed in a formatted table and can be optionally written to an InfluxDB database. The application also supports asynchronous handling of trade data and manages the connections to the exchanges. The main components include the MarketAggregator class for data aggregation and statistics calculation, and the CCXTManager class for handling the connections to the exchanges and processing trades. The application monitors selected trading pairs on a predefined list of exchanges and reports the aggregated data in real-time.

# Table of Market Statistics
### Δ - delta (buys-sells)
(2 mins of trade activity)
![asdasdasd](https://user-images.githubusercontent.com/23511285/236011966-e0c60537-1781-42bc-bfba-83d770cd7de6.png)