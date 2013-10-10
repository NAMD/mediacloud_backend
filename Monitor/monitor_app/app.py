#----------------------------------------------------------------------------#
# Imports.
#----------------------------------------------------------------------------#

from flask import Flask,render_template, jsonify, flash, request, redirect, url_for, Response, make_response  # do not use '*'; actually input the dependencies.
from flask.ext.sqlalchemy import SQLAlchemy
import logging
from logging import Formatter, FileHandler
from forms import *
import models
import json
import pymongo
from bson import json_util
import base64

#----------------------------------------------------------------------------#
# App Config.
#----------------------------------------------------------------------------#

app = Flask(__name__)
app.config.from_object('config')
db = SQLAlchemy(app)

# Automatically tear down SQLAlchemy.
'''
@app.teardown_request
def shutdown_session(exception=None):
    db_session.remove()
'''

# Login required decorator.
'''
def login_required(test):
    @wraps(test)
    def wrap(*args, **kwargs):
        if 'logged_in' in session:
            return test(*args, **kwargs)
        else:
            flash('You need to login first.')
            return redirect(url_for('login'))
    return wrap
'''
@app.route('/dbstats')
def db_stats():
    """
    From GET take:  login, password : database credentials(optional, currently ignored)

    Return json with database stats,as returned by mongo (db.stats())
    """
    conf = models.Configuration.query.first()
    if not conf:
        return redirect(url_for('config'))
    host = conf.mongohost

    try:
        conn = pymongo.Connection(host=host, port=27017)
        db = conn.MCDB
        resp = db.command({'dbstats': 1})
        json_response = json.dumps({'data': resp}, default=json_util.default)
    except Exception, e:
        json_response = json.dumps({'error': repr(e)})
    finally:
        conn.disconnect()

    return Response(json_response, mimetype='application/json')
#----------------------------------------------------------------------------#
# Controllers.
#----------------------------------------------------------------------------#

@app.route('/')
def home():
    conf = models.Configuration.query.first()
    if conf:
        data = {
            'host': conf.mongohost,
        }
    else:
        data = {}
    return render_template('pages/placeholder.home.html', data=data)

@app.route('/about')
def about():
    return render_template('pages/placeholder.about.html')

@app.route('/login')
def login():
    form = LoginForm(request.form)
    return render_template('forms/login.html', form = form)

@app.route('/register')
def register():
    form = RegisterForm(request.form)
    return render_template('forms/register.html', form = form)

@app.route('/forgot')
def forgot():
    form = ForgotForm(request.form)
    return render_template('forms/forgot.html', form = form)

@app.route('/config', methods=['GET', 'POST'])
def config():
    form = ConfigurationForm(request.form)
    if request.method == 'POST':
        c = models.Configuration(mongohost=form.dbhost.data, mongouser=form.dbuser.data, mongopasswd=form.dbpasswd.data, pyplnhost=form.pyplnhost.data,
                          pyplnuser=form.pyplnuser.data, pyplnpasswd=form.pyplnpasswd.data)
        db.session.add(c)
        db.session.commit()
        flash('Configuration saved')
        return redirect(url_for('home'))
    else:
        conf = models.Configuration.query.first()
        if conf:
            form.dbhost.data = conf.mongohost
            form.dbuser.data = conf.mongouser
            form.dbpasswd.data = conf.mongopasswd
            form.pyplnhost.data = conf.pyplnhost
            form.pyplnuser.data = conf.pyplnuser
            form.pyplnpasswd.data = conf.pyplnpasswd
    return render_template('forms/config.html', form=form)

@app.route('/feeds')
def feeds():
    conf = models.Configuration.query.first()
    C = pymongo.MongoClient(conf.mongohost)
    nfeeds = C.MCDB.feeds.count()
    feeds = fetch_docs('feeds')


    return render_template('pages/feeds.html',nfeeds=nfeeds, feeds=feeds)

@app.route('/articles')
def articles():
    conf = models.Configuration.query.first()
    C = pymongo.MongoClient(conf.mongohost)
    articles = fetch_docs('articles')
    return render_template('pages/articles.html', articles=articles)

# Utility functions

def fix_json_output(json_obj):
    """
        Handle binary data in output json, because pymongo cannot encode them properly (generating UnicodeDecode exceptions)
    """
    def _fix_json(d):
        if d in [None, [], {}]: #if not d: breaks empty Binary
            return d
        data_type = type(d)
        if data_type == list:
            data = []
            for item in d:
                data.append(_fix_json(item))
            return data
        elif data_type == dict:
            data = {}
            for k in d:
                data[_fix_json(k)] = _fix_json(d[k])
            return data
        elif data_type == pymongo.binary.Binary:
            ud = base64.encodestring(d)
            return { '$binary' : ud, '$type': d.subtype }
        else:
            return d

    return _fix_json(json_obj)

def fetch_docs(colname, limit=100):
    """
    Query MongoDB in the collection specified

    Return json with requested data or error
    """
    conf = models.Configuration.query.first()
    host = conf.mongohost
    try:
        conn = pymongo.Connection(host = host)
        db = conn.MCDB
        coll = db['colname']
        resp = {}
        # query = json.loads(request.GET['q'], object_hook=json_util.object_hook)
        # limit = 10
        # sort = None
        # if 'limit' in request.GET:
        #     limit = int(request.GET['limit'])
        # skip = 0
        # if 'skip' in request.GET:
        #     skip = int(request.GET['skip'])
        # if 'sort' in request.GET:
        #     sort = json.loads(request.GET['sort'])
        cur = coll.find(limit=limit)
        cnt = cur.count()
        # if sort:
        #     cur = cur.sort(sort)
        resp = [a for a in cur]
        json_response = json.dumps({'data': fix_json_output(resp), 'meta': {'count': cnt}}, default=json_util.default)
    except Exception, e:
        print e
        import traceback
        traceback.print_stack()
        json_response = json.dumps({'error': repr(e)})
    finally:
        conn.disconnect()

    #resp = Response(json_response, mimetype='application/json' )
    #resp['Cache-Control'] = 'no-cache'
    return json_response


# Error handlers.

@app.errorhandler(500)
def internal_error(error):
    #db_session.rollback()
    return render_template('errors/500.html'), 500

@app.errorhandler(404)
def internal_error(error):
    return render_template('errors/404.html'), 404

if not app.debug:
    file_handler = FileHandler('error.log')
    file_handler.setFormatter(Formatter('%(asctime)s %(levelname)s: %(message)s '
    '[in %(pathname)s:%(lineno)d]'))
    app.logger.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.info('errors')

#----------------------------------------------------------------------------#
# Launch.
#----------------------------------------------------------------------------#

# Default port:
if __name__ == '__main__':
    app.run(debug=True)

# Or specify port manually:
'''
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
'''
