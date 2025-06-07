# filename: server.py

from flask import Flask, jsonify
import requests
from datetime import datetime, timezone
import time

app = Flask(__name__)

@app.route('/get-time', methods=['GET'])
def get_time():
    # URL of the NPL India NTP client API
    url = "http://nplindia.in/cgi-bin/ntp_client?1746089691.568"

    try:
        # Record time before sending the request (in seconds with milliseconds)
        t_start = time.time()
        print(f"Request sent at: {datetime.utcfromtimestamp(t_start).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")

        # Send GET request to the NPL India NTP client API
        response = requests.get(url)
        response.raise_for_status()

        # Record time after receiving the response
        t_end = time.time()
        print(f"Response received at: {datetime.utcfromtimestamp(t_end).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")

        # Calculate the latency
        latency = t_end - t_start
        print(f"Round-trip latency: {latency*1000:.3f} ms")

        # Parse JSON response
        data = response.json()

        # Extract the 'nstt' value (NTP server time)
        ntp_time = data['nstt']
        ntp_datetime = datetime.utcfromtimestamp(ntp_time)
        print(f"Received NTP time: {ntp_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC")

        # Calculate the time range
        half_latency = latency / 2
        lower_bound = ntp_time - half_latency
        upper_bound = ntp_time + half_latency
        print(f"Time range: [{datetime.utcfromtimestamp(lower_bound).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC, {datetime.utcfromtimestamp(upper_bound).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC]")

        # Return as JSON
        return jsonify({
            "original_utc_time": ntp_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": ntp_time,
            "latency_ms": latency * 1000,
            "time_range": {
                "lower_bound_timestamp": lower_bound,
                "upper_bound_timestamp": upper_bound,
                "lower_bound_utc": datetime.utcfromtimestamp(lower_bound).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "upper_bound_utc": datetime.utcfromtimestamp(upper_bound).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
