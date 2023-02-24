# app/models.py

from app.database import db
from datetime import datetime

class Store(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp_utc = db.Column(db.DateTime)
    status = db.Column(db.String(10))

class BusinessHours(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.Integer)
    day_of_week = db.Column(db.Integer)
    start_time_local = db.Column(db.Time)
    end_time_local = db.Column(db.Time)

class Timezone(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.Integer)
    timezone_str = db.Column(db.String(50))

class Report(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    report_id = db.Column(db.String(255), nullable=False, unique=True)
    status = db.Column(db.String(50), nullable=False)
    data = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

    def to_dict(self):
        return {
            'id': self.id,
            'report_id': self.report_id,
            'status': self.status,
            'created_at': self.created_at.isoformat() + 'Z',
            'completed_at': self.completed_at.isoformat() + 'Z' if self.completed_at else None
        }

