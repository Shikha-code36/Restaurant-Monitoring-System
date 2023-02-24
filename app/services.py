# app/services.py

from app.database import db
import threading
import time
import functools
import pandas as pd
import pytz
from datetime import datetime, timedelta
from app.models import Store, BusinessHours, Timezone

@functools.lru_cache(maxsize=1, typed=False) # cache for 1 hour
def import_data():
    batch_size = 100
    from app import create_app
    app = create_app()

    with app.app_context():
        db.create_all()

        # Import data from CSVs
        stores_csv = pd.read_csv('data/stores.csv', chunksize=batch_size)
        business_hours_csv = pd.read_csv('data/business_hours.csv', chunksize=batch_size)
        timezones_csv = pd.read_csv('data/timezones.csv', chunksize=batch_size)

        # Define timezone dictionary
        timezone_dict = {}
        for _, row in pd.concat(timezones_csv).iterrows():
            timezone_dict[row['store_id']] = pytz.timezone(row['timezone_str'])

        # Insert data into the database
        for stores_df in stores_csv:
            stores_df = stores_df.dropna(subset=['timestamp_utc'])
            stores_df['timestamp_utc'] = pd.to_datetime(stores_df['timestamp_utc'])

            for i, row in stores_df.iterrows():
                store_id = row['store_id']
                status = row['status']
                timezone = timezone_dict.get(store_id, pytz.timezone('America/Chicago'))
                timestamp_local = row['timestamp_utc'].astimezone(timezone)
                store = Store(timestamp_utc=row['timestamp_utc'], status=status)
                db.session.add(store)

                if (i+1) % batch_size == 0:
                    db.session.commit()

            db.session.commit()

        print(f"Number of stores: {len(Store.query.all())}")

        for business_hours_df in business_hours_csv:
            for i, row in business_hours_df.iterrows():
                start_time = pd.to_datetime(row['start_time_local']).time()
                end_time = pd.to_datetime(row['end_time_local']).time()
                business_hours = BusinessHours(store_id=row['store_id'], day_of_week=row['day'], start_time_local=start_time, end_time_local=end_time)
                db.session.add(business_hours)

                if (i+1) % batch_size == 0:
                    db.session.commit()

            db.session.commit()

        for timezones_df in timezones_csv:
            for i, row in timezones_df.iterrows():
                timezone = Timezone(store_id=row['store_id'], timezone_str=row['timezone_str'])
                db.session.add(timezone)

                if (i+1) % batch_size == 0:
                    db.session.commit()

            db.session.commit()

# run import_data() in a separate thread
def run_import_data():
    import_data()

# start the import_data() function in a separate thread
t = threading.Thread(target=run_import_data)
t.start()

# start a timer to refresh the cache every hour
def refresh_cache():
    while True:
        time.sleep(3600)  # sleep for 1 hour
        run_import_data()  # call the function to refill the cache

# start the refresh_cache() function in a separate thread
t2 = threading.Thread(target=refresh_cache)
t2.start()

def get_store_uptime_downtime(store_id, start_date, end_date):
    """
    Computes the uptime and downtime for a given store within a given time range.
    """
    store = Store.query.get(store_id)

    # Retrieve the store's timezone
    timezone_str = Timezone.query.filter_by(store_id=store_id).first().timezone_str
    timezone = pytz.timezone(timezone_str)

    # Compute business hours in local timezone for each day within the given time range
    business_hours = {}
    for day_offset in range((end_date - start_date).days + 1):
        date = start_date + timedelta(days=day_offset)
        day_of_week = date.weekday()
        local_start_time = datetime.combine(date, BusinessHours.query.filter_by(store_id=store_id, day_of_week=day_of_week).first().start_time_local)
        local_end_time = datetime.combine(date, BusinessHours.query.filter_by(store_id=store_id, day_of_week=day_of_week).first().end_time_local)
        business_hours[date] = (local_start_time.astimezone(timezone), local_end_time.astimezone(timezone))

    # Retrieve store status changes within the given time range
    status_changes = store.status_changes.filter(Store.timestamp_utc.between(start_date, end_date)).order_by(Store.timestamp_utc).all()

    # Initialize counters for uptime and downtime
    uptime = timedelta()
    downtime = timedelta()

    # Compute uptime and downtime based on status changes and business hours
    last_status = None
    for i, status_change in enumerate(status_changes):
        if i == 0:
            last_status = status_change.status
            continue

        time_diff = status_change.timestamp_utc - status_changes[i - 1].timestamp_utc

        if last_status == "open":
            # Compute downtime during non-business hours
            for j in range((status_changes[i - 1].timestamp_utc.date() - start_date).days, (status_change.timestamp_utc.date() - start_date).days):
                date = start_date + timedelta(days=j)
                if business_hours[date][1] < business_hours[date][0]:
                    downtime += timedelta(hours=24) - (business_hours[date][1] - business_hours[date][0])
                else:
                    downtime += max(timedelta(), business_hours[date][0] - business_hours[date][1])
            uptime += time_diff
        else:
            # Compute uptime during business hours
            for j in range((status_changes[i - 1].timestamp_utc.date() - start_date).days, (status_change.timestamp_utc.date() - start_date).days):
                date = start_date + timedelta(days=j)
                if business_hours[date][1] < business_hours[date][0]:
                    uptime += timedelta(hours=24) - (business_hours[date][1] - business_hours[date][0])
                else:
                    uptime += max(timedelta(), business_hours[date][1] - business_hours[date][0])
            downtime += time_diff

        last_status = status_change.status

    # Compute uptime and downtime for the last status change to the end of the time range
    # Compute uptime and downtime for the last status change to the end of the time range
    if last_status == "open":
        for j in range((status_changes[-1].timestamp_utc.date() - start_date).days, (end_date - start_date).days + 1):
            date = start_date + timedelta(days=j)
            if business_hours[date][1] is not None:
                downtime += business_hours[date][1] - business_hours[date][0]

    else:
        for j in range((status_changes[-1].timestamp_utc.date() - start_date).days, (end_date - start_date).days + 1):
            date = start_date + timedelta(days=j)
            if business_hours[date][0] is not None:
                uptime += business_hours[date][1] - business_hours[date][0]

    return uptime, downtime
