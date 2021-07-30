import os
import datetime as dt
import multiprocessing
from flask import Flask, jsonify, request, send_from_directory, url_for
from werkzeug.utils import secure_filename, redirect

from main import Main


app = Flask(__name__)
keyspace, main = "mykspc", None

app.config["CSV_OUTPUT"] = ""
app.config["CSV_TRANSACTIONS"] = "transactions/"
app.config["CSV_PRICES"] = "prices/"

offset = dt.timezone(dt.timedelta(hours=3))
time = dt.datetime.now(offset)


point = time.replace(hour=18, minute=10)
date_substring = "output/alerts_" + str(dt.datetime.strptime(str(dt.datetime.now().date()), '%Y-%m-%d').strftime('%d%m%Y')) + ".csv"

if time < point:
    up_create_DB = False
    if not os.path.isdir("output"):
        up_create_DB = True
    elif not os.path.isfile(date_substring):
        up_create_DB = True
elif time >= point:
    if not os.path.isdir("output"):
        up_create_DB = True
    elif not os.path.isfile(date_substring):
        up_create_DB = True
    else:
        up_create_DB = False


def to_status(f):
    if f == 1:
        return "processing"
    if f == 2:
        return "waitForFile"
    if f == 3:
        return "done"


if not os.path.isdir("prices") and not os.path.isdir("transactions"):
    os.mkdir("prices")
    os.mkdir("transactions")


def start():
    global main, keyspace, up_create_DB
    if up_create_DB:
        n = multiprocessing.Value('d', 0.0)
        s = multiprocessing.Value("i", 1)
        main = Main(up_create_DB, keyspace, n, s)
        process = multiprocessing.Process(target=main.run, args=(n, s,))
        process.start()
    else:
        n = multiprocessing.Value('d', 0.0)
        s = multiprocessing.Value("i", 1)
        main = Main(up_create_DB, keyspace, n, s)
        process = multiprocessing.Process(target=main.run, args=(n, s,))
        process.start()


@app.before_first_request
def before_request():
    start()


@app.route('/api/alerts', methods=['GET'])
def get_simpl_alerts_list():
    return jsonify(main.py_driver.get_data(main.alert.table_name))


@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({"status": to_status(main.get_s_status()), "value":  main.alert.get_qe()})


@app.route('/api/alertsCSV', methods=['GET'])
def get_alert_csv():
    return send_from_directory(app.config["CSV_OUTPUT"], path=date_substring, as_attachment=True, max_age=0)


@app.route('/api/transactions', methods=['POST', 'GET'])
def post_transactions_csv():
    if request.method == "GET":
        alerts_id = request.args.get('alertId', default='', type=str)
        return jsonify(main.py_driver.get_data(alerts_id))
    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            if file:
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['CSV_TRANSACTIONS'], filename))
                return redirect(url_for("post_transactions_csv"))


@app.route('/api/prices', methods=['POST'])
def post_prices_csv():
    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            if file:
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['CSV_PRICES'], filename))
                return redirect(url_for("post_prices_csv"))


if __name__ == '__main__':
    app.run()
