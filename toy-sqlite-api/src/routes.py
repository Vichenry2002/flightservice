from flask import Blueprint, request, jsonify
import sqlite3

main = Blueprint('main', __name__)

@main.route('/data')
def get_data():
    instruments = request.args.getlist('instruments')
    indicators = request.args.getlist('indicators')
    if 'timestamp' not in indicators:
        indicators.append('timestamp')
    
    data = {}
    for instrument in instruments:
        query = f"SELECT {', '.join(indicators)} FROM company WHERE symbol = ?"
        result = query_db(query, (instrument,))
        if result:
            # Assumes that result is a list of tuples, even if one tuple is returned
            data[instrument] = dict(zip(indicators, result[0]))
        else:
            data[instrument] = {indicator: None for indicator in indicators}

    response = {
        "data": data
    }
    return jsonify(response)

def query_db(query, args=(), one=False):
    conn = sqlite3.connect('toy_fi_data.db')
    cursor = conn.cursor()
    cursor.execute(query, args)
    rv = cursor.fetchall()
    cursor.close()
    conn.close()
    return (rv[0] if rv else None) if one else rv
