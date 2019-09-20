from flask import Flask
from flask_login import LoginManager
from flask_pymongo import PyMongo
from flask_bcrypt import Bcrypt

app = Flask(__name__, static_url_path='/assets', static_folder="assets")
app.config.from_object('config')
mongo = PyMongo(app=app,retryWrites=False)

login_manager = LoginManager()
login_manager.init_app(app)

bcrypt = Bcrypt(app)

#from .views import *
from .employee_page import *
from .payment_page import *
from .repayment_page import *
from .subscription_page import *
from .login_page import *
from .home_page import *