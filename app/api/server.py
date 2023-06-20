from flask import Flask, request, jsonify

from api.influx import InfluxDB

class Server:
    def __init__(self, influx: InfluxDB) -> None:
        self.app = Flask(__name__)
        self.db = influx  # initialize InfluxDB client
        
        self.register_routes()

    def register_routes(self):
        @self.app.route('/ohlcv', methods=['GET'])
        def get_ohlcv():
            exchange = request.args.get('exchange')
            symbol = request.args.get('symbol')
            timeframe = request.args.get('timeframe')
            start = request.args.get('start')
            stop = request.args.get('stop')

            df = self.db.query_candles_from_influxdb(exchange, symbol, timeframe, start, stop)
            return jsonify(df.to_dict(orient='records'))  # convert DataFrame to dict and then to JSON
        
    def run(self):
        self.app.run(debug=True)