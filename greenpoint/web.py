import asyncio
import json
import datetime
import decimal

import flask

import flask_restful

from greenpoint import portfolio


app = flask.Flask(__name__)


class Portfolio(flask_restful.Resource):
    @staticmethod
    def get():
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        f = portfolio.get_status_for_all(loop)
        status = loop.run_until_complete(f)
        return [dict(p) for p in status]


api = flask_restful.Api(app)
api.add_resource(Portfolio, '/portfolio')


@app.route('/')
def hello_world():
    return 'Hello, World!'


def _to_primitive(obj):
    if isinstance(obj, (str, int, type(None), bool, float)):
        return obj
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if isinstance(obj, dict):
        return {_to_primitive(k): _to_primitive(v)
                for k, v in obj.items()}
    if hasattr(obj, 'items'):
        return _to_primitive(dict(obj.items()))
    if hasattr(obj, '__iter__'):
        return list(map(_to_primitive, obj))
    return obj


@api.representation('application/json')
def output_json(data, code, headers=None):
    resp = flask.make_response(json.dumps(_to_primitive(data)), code)
    resp.headers.extend(headers or {})
    return resp
