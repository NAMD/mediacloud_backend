from flask.ext.wtf import Form
from wtforms import TextField, PasswordField
from wtforms.validators import Required, EqualTo, Length

# Set your classes here.

class RegisterForm(Form):
    name        = TextField('Username', validators = [Required(), Length(min=6, max=25)])
    email       = TextField('Email', validators = [Required(), Length(min=6, max=40)])
    password    = PasswordField('Password', validators = [Required(), Length(min=6, max=40)])
    confirm     = PasswordField('Repeat Password', [Required(), EqualTo('password', message='Passwords must match')])

class LoginForm(Form):
    name        = TextField('Username', [Required()])
    password    = PasswordField('Password', [Required()])

class ForgotForm(Form):
    email       = TextField('Email', validators = [Required(), Length(min=6, max=40)])

class ConfigurationForm(Form):
    dbhost      = TextField('MongoDB Host', validators = [Required()])
    dbuser      = TextField('MongoDB Username', [Required()])
    dbpasswd    = PasswordField('MongoDB Password', [Required()])
    pyplnhost   = TextField('PyPLN Host', validators = [Required()])
    pyplnuser      = TextField('PyPLN Username', [Required()])
    pyplnpasswd    = PasswordField('PyPLN Password', [Required()])

