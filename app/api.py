import time
import flask
from flask import Response,request,abort
import os
import watchtower,logging
import json
from service import tfidf_updation,similarity_score
from flask_executor import Executor
import ssl
from functools import wraps
from healthcheck import HealthCheck, EnvironmentDump


os.environ["FLASK_APP"] = 'api.py'
os.environ['AWS_DEFAULT_REGION'] = 'ap-south-1'
# logging.basicConfig(level=logging.INFO)

app = flask.Flask(__name__)

health = HealthCheck()
envdump = EnvironmentDump()
app.config.from_envvar('APP_CONFIG_FILE')
app.config['APPLICATION_ROOT'] = '/address-verification/api/v1'
os.environ['AWS_ACCESS_KEY_ID'] = app.config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = app.config['AWS_SECRET_ACCESS_KEY']
app.add_url_rule(app.config['APPLICATION_ROOT'] + "/healthcheck", "healthcheck", view_func=lambda: health.run())
app.add_url_rule(app.config['APPLICATION_ROOT'] + "/environment", "environment", view_func=lambda: envdump.run())
executor = Executor(app)
handler = watchtower.CloudWatchLogHandler(app.config['LOG_GROUP_NAME'])
app.logger.addHandler(handler)
logging.getLogger("werkzeug").addHandler(handler)
@app.route(app.config['APPLICATION_ROOT']+'/dummy', methods=['GET'])
def home():
    logging.getLogger("werkzeug").info("Home Page API called")
    return "<h1>address-verification-service</h1>"


def require_appkey(view_function):
    @wraps(view_function)
    # the new, post-decoration function. Note *args and **kwargs here.
    def decorated_function(*args, **kwargs):
        with open('api.key', 'r') as apikey:
            key=apikey.read().replace('\n', '')
        #if request.args.get('key') and request.args.get('key') == key:
        if request.headers.get('x-api-key') and request.headers.get('x-api-key') == key:
            return view_function(*args, **kwargs)
        else:
            abort(401)
    return decorated_function


@app.route(app.config['APPLICATION_ROOT']+'/similar-address', methods=['GET'])
@require_appkey
def get_similar_address():
    try:
        content_type = request.headers.get('Content-Type')
        if (content_type == 'application/json'):
            json_content = request.json
            address_id = json_content['address_id']
            warehouse_id = json_content['warehouse_id']
            address = json_content['address']

            similar_result = similarity_score.compute_siilarity_score(app.config,address_id,warehouse_id,address)
            response = app.response_class(response=json.dumps(similar_result),
                                  status=200,
                                  mimetype='application/json')
            return response

        else:
            return Response(500)
    except Exception as e:
        print(e)
        return Response(status=500)


@app.route(app.config['APPLICATION_ROOT']+'/tfidf-updation', methods=['GET'])
def update_tfidf():
    try:
        # tfidf_updation.warehouse_address_tfidf(app.config)
        executor.submit(tfidf_updation.warehouse_address_tfidf, app.config)
        return Response(status=200, mimetype='application/json')
    except Exception as e:
        print(e)
        return Response(status=500)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)