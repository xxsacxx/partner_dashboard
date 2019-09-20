from app import app
from flask_debug import Debug

if __name__ == "__main__":
    Debug(app)
    app.run(debug=True)
