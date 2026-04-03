from flask_sqlalchemy import SQLAlchemy
from datetime import date

db = SQLAlchemy()


class Transaction(db.Model):
    __tablename__ = "transactions"

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False, index=True)
    description = db.Column(db.String(500), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    account = db.Column(db.String(100), nullable=False, index=True)
    category = db.Column(db.String(100), default="", index=True)

    # Composite duplicate key
    dup_key = db.Column(db.String(1000), unique=True, index=True)

    def to_dict(self):
        return {
            "id": self.id,
            "date": self.date.isoformat(),
            "description": self.description,
            "amount": self.amount,
            "account": self.account,
            "category": self.category,
        }

    @staticmethod
    def build_dup_key(dt, description, amount, account):
        d = dt.strftime("%Y-%m-%d") if isinstance(dt, date) else str(dt)
        desc = str(description).strip().lower()
        amt = str(round(float(amount), 2))
        return f"{d}|{desc}|{amt}|{account}"


class Budget(db.Model):
    __tablename__ = "budgets"

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False, index=True)
    month = db.Column(db.Integer, nullable=False)  # 1-12
    category = db.Column(db.String(100), nullable=False, index=True)
    amount = db.Column(db.Float, nullable=False, default=0.0)

    __table_args__ = (
        db.UniqueConstraint("year", "month", "category", name="uq_budget_entry"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "year": self.year,
            "month": self.month,
            "category": self.category,
            "amount": self.amount,
        }


class CategoryGroupMapping(db.Model):
    __tablename__ = "category_group_mappings"

    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(100), nullable=False, unique=True, index=True)
    group = db.Column(db.String(100), nullable=False, index=True)

    def to_dict(self):
        return {
            "id": self.id,
            "category": self.category,
            "group": self.group,
        }
