from flask import Flask
from db.initialize import initialize_toy_db
from src.routes import main 

def create_app():
    app = Flask(__name__)
    initialize_toy_db()
    app.register_blueprint(main, url_prefix='/api')
    return app

app = create_app()

if __name__ == '__main__':
    app.run(debug=True)