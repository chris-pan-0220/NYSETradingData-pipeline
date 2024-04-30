from flask import Flask, render_template, request, abort
from functools import wraps
import time

app = Flask(__name__)

def restrict_ip_subnet(subnet):
    def decorator(f):
        @wraps(f)  # This preserves the original function's name and docstring
        def decorated_function(*args, **kwargs):
            if not request.remote_addr.startswith(subnet):
                abort(403)  # Forbidden
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.route('/')
@restrict_ip_subnet('140.115.')  # Only allow access from IPs starting with 140.115.
def index():
    return render_template('index.html')

@app.route('/download')
@restrict_ip_subnet('140.115.')
def download():
    try:
        with open('download.log', 'r') as file:
            content = file.read()
    except FileNotFoundError:
        content = "File not found."
    # return content
    return render_template('log.html', content=content, last_updated=time.ctime())

@app.route('/proc-bbo')
@restrict_ip_subnet('140.115.')
def bbo():
    try:
        with open('bbo.log', 'r') as file:
            content = file.read()
    except FileNotFoundError:
        content = "File not found."
    # return content
    return render_template('log.html', content=content, last_updated=time.ctime())

@app.route('/proc-except-bbo')
@restrict_ip_subnet('140.115.')
def except_bbo():
    try:
        with open('except_bbo.log', 'r') as file:
            content = file.read()
    except FileNotFoundError:
        content = "File not found."
    # return content
    return render_template('log.html', content=content, last_updated=time.ctime())

if __name__ == "__main__":
    app.run(host='140.115.160.52', debug=False)  # Ensure debug mode is off
