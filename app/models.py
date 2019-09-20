from flask_login import UserMixin


class User(UserMixin):
    email = str()
    password = str()
    authenticated = bool(False)

    def __init__(self, email, password):
        self.email = email
        self.password = password

    def is_active(self):
        return True

    def get_id(self):
        return self.email

    def is_authenticated(self):
        return self.authenticated

    def is_anonymous(self):
        return False
