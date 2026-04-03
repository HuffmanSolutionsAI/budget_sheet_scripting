import os
from flask import Flask
from config import Config
from models import db


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    db.init_app(app)

    from routes import main_bp, api_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix="/api")

    with app.app_context():
        db.create_all()
        _seed_default_categories(db)

    return app


def _seed_default_categories(db):
    from models import CategoryGroupMapping

    if CategoryGroupMapping.query.count() > 0:
        return

    defaults = {
        "Income": [
            "Salary", "Bonus", "Interest", "Reimbursement",
            "Employer Benefit", "Employer Benefit Offset",
        ],
        "Necessities": [
            "Groceries", "Gas", "Utilities", "Phone",
            "Personal Care", "Medical",
        ],
        "Discretionary": [
            "Restaurants", "Entertainment", "Shopping",
            "Activities", "Travel", "Subscriptions", "Gifts",
        ],
        "Fixed Expenses": [
            "Rent", "Insurance", "Car Payment", "Gym",
        ],
        "Taxes": [
            "Federal Income Tax", "State Income Tax",
            "Social Security", "Medicare",
        ],
        "Savings & Investments": [
            "401k", "HSA", "Roth IRA", "Brokerage",
        ],
        "Work": ["Work Expenses"],
        "Transfer": ["Transfer"],
        "Other": ["Other", "Uncategorized"],
    }

    for group, categories in defaults.items():
        for cat in categories:
            db.session.add(CategoryGroupMapping(category=cat, group=group))
    db.session.commit()


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5000)
