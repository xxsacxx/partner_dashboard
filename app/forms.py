from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField
from wtforms.validators import DataRequired, email


class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), email('Please enter a valid email address.')])
    password = PasswordField('Password', validators=[DataRequired()])
