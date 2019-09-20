from pymongo.errors import BulkWriteError

from app import bcrypt
from flask import current_app
from app import app, mongo
from getpass import getpass
import sys
from app.constants import Payment


def create_admin():
    """Main entry point for script."""
    users = mongo.db.User.find({})
    with app.app_context():
        # mongo.metadata.create_all(db.engine)
        if len(list(users)) > 0:
            print('A user already exists! Create another? (y/n):', )
            create = input()
            if create == 'n':
                return

        print('Enter email address: ', )
        email = input()
        password = getpass()
        assert password == getpass('Password (again):')
        user = mongo.db.User.insert_one({
            "email": email,
            "password": bcrypt.generate_password_hash(password),
            "is_authenticated": False
        })
        print('User added.')


def load_payments():
    print("Loading Payments")
    pay_list = list()

    pay_list.append(Payment(number="KL0001234567", month="Aug 2019", vendor="watermelon", value="20,000", status=True,
                            pay_type=1, tnx_id="AD95231808").Payment_dict)
    pay_list.append(Payment(number="KL0001234567", month="Aug 2019", vendor="Banana", value="20,000", status=False,
                            pay_type=1, tnx_id="AC1231113").Payment_dict)
    pay_list.append(Payment(number="KL0001234567", month="Aug 2019", vendor="watermelon", value="20,000", status=True,
                            pay_type=0, tnx_id="FD1231234").Payment_dict)
    pay_list.append(Payment(number="KL0001234567", month="Aug 2019", vendor="Apple", value="20,000", status=False,
                            pay_type=1, tnx_id="MA1234566").Payment_dict)
    pay_list.append(Payment(number="KL0001234567", month="Aug 2019", vendor="watermelon", value="20,000", status=True,
                            pay_type=0, tnx_id="LO12343123").Payment_dict)

    try:
        response = mongo.db.Payments.insert_many(pay_list)
    except BulkWriteError as bwe:
        print(bwe.details)
        raise
    if response.acknowledged:
        print("success")
    else:
        print("Error from server")


if __name__ == "__main__":
    Func_dict = {
        1: create_admin,
        2: load_payments,
    }
    print("""
    Type Input
        1) Create Login
        2) Load Payments

    """)
    a = input("Your Response : ")
    try:
        a = int(a)
        if a < 1 or a>2:
            raise Exception
        Func_dict[int(a)]()
    except Exception as e:
        print("Wrong Response")