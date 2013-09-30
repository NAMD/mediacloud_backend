#----------------------------------------------------------------------------#
# Imports.
#----------------------------------------------------------------------------#

from flask import *  # do not use '*'; actually input the dependencies.
from flask.ext.sqlalchemy import SQLAlchemy
import logging
from logging import Formatter, FileHandler
from forms import *
import models
import pymongo

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
#----------------------------------------------------------------------------#
# Controllers.
#----------------------------------------------------------------------------#

@app.route('/')
def home():
    #TODO: fetch from database total number of feeds an total number of articles downloaded
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
    return render_template('forms/config.html', form=form)

@app.route('/feeds')
def feeds():
    return render_template('pages/feeds.html')

@app.route('/articles')
def articles():
    return render_template('pages/articles.html')

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
