import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from pymongo import MongoClient

fake = Faker()

MONGO_URI = "mongodb://mongo_user:mongo_password@mongodb:27017/app_db?authSource=admin"
DB_NAME = "app_db"

PAGES = ["/home", "/products", "/products/42", "/cart", "/checkout",
         "/account", "/search", "/blog", "/about", "/contact"]
ACTIONS = ["login", "view_product", "add_to_cart", "remove_from_cart",
           "checkout", "logout", "search", "view_blog"]
EVENT_TYPES = ["click", "page_view", "scroll", "form_submit", "error", "purchase"]
ISSUE_TYPES = ["payment", "shipping", "account", "product_quality", "return", "technical"]
STATUSES = ["open", "in_progress", "resolved", "closed"]
MODERATION_STATUSES = ["pending", "approved", "rejected"]
FLAGS = ["contains_images", "contains_links", "spam_suspected", "profanity_suspected"]
DEVICES = ["mobile", "desktop", "tablet"]
PRODUCTS = [f"prod_{i}" for i in range(100, 400)]


def _utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_user_sessions(n: int = 500) -> list[dict]:
    docs = []
    for i in range(1, n + 1):
        start = fake.date_time_between(start_date="-90d", end_date="now", tzinfo=timezone.utc)
        end = start + timedelta(minutes=random.randint(1, 120))
        docs.append({
            "session_id": f"sess_{i:05d}",
            "user_id": f"user_{random.randint(1, 100):03d}",
            "start_time": _utc(start),
            "end_time": _utc(end),
            "pages_visited": random.sample(PAGES, k=random.randint(1, 6)),
            "device": random.choice(DEVICES),
            "actions": random.sample(ACTIONS, k=random.randint(1, 5)),
        })
    return docs


def make_event_logs(n: int = 2000) -> list[dict]:
    docs = []
    for i in range(1, n + 1):
        ts = fake.date_time_between(start_date="-90d", end_date="now", tzinfo=timezone.utc)
        docs.append({
            "event_id": f"evt_{i:05d}",
            "timestamp": _utc(ts),
            "event_type": random.choice(EVENT_TYPES),
            "details": random.choice(PAGES + PRODUCTS),
            "user_id": f"user_{random.randint(1, 100):03d}",
            "session_id": f"sess_{random.randint(1, 500):05d}",
        })
    return docs


def make_support_tickets(n: int = 300) -> list[dict]:
    docs = []
    for i in range(1, n + 1):
        created = fake.date_time_between(start_date="-90d", end_date="-1d", tzinfo=timezone.utc)
        updated = created + timedelta(hours=random.randint(0, 48))
        status = random.choice(STATUSES)
        msgs = [
            {
                "sender": "user",
                "message": fake.sentence(),
                "timestamp": _utc(created),
            }
        ]
        if status != "open":
            msgs.append({
                "sender": "support",
                "message": fake.sentence(),
                "timestamp": _utc(created + timedelta(hours=random.randint(1, 24))),
            })
        docs.append({
            "ticket_id": f"ticket_{i:04d}",
            "user_id": f"user_{random.randint(1, 100):03d}",
            "status": status,
            "issue_type": random.choice(ISSUE_TYPES),
            "messages": msgs,
            "created_at": _utc(created),
            "updated_at": _utc(updated),
        })
    return docs


def make_user_recommendations(n: int = 100) -> list[dict]:
    docs = []
    for i in range(1, n + 1):
        updated = fake.date_time_between(start_date="-7d", end_date="now", tzinfo=timezone.utc)
        docs.append({
            "user_id": f"user_{i:03d}",
            "recommended_products": random.sample(PRODUCTS, k=random.randint(3, 10)),
            "last_updated": _utc(updated),
        })
    return docs


def make_moderation_queue(n: int = 400) -> list[dict]:
    docs = []
    for i in range(1, n + 1):
        submitted = fake.date_time_between(start_date="-90d", end_date="now", tzinfo=timezone.utc)
        docs.append({
            "review_id": f"rev_{i:05d}",
            "user_id": f"user_{random.randint(1, 100):03d}",
            "product_id": random.choice(PRODUCTS),
            "review_text": fake.sentence(nb_words=random.randint(8, 30)),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(MODERATION_STATUSES),
            "flags": random.sample(FLAGS, k=random.randint(0, 2)),
            "submitted_at": _utc(submitted),
        })
    return docs


def generate_all():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    collections = {
        "user_sessions": make_user_sessions(),
        "event_logs": make_event_logs(),
        "support_tickets": make_support_tickets(),
        "user_recommendations": make_user_recommendations(),
        "moderation_queue": make_moderation_queue(),
    }

    for name, docs in collections.items():
        col = db[name]
        col.drop()
        col.insert_many(docs)
        print(f"  {name}: inserted {len(docs)} documents")

    db["user_sessions"].create_index("session_id", unique=True)
    db["event_logs"].create_index("event_id", unique=True)
    db["support_tickets"].create_index("ticket_id", unique=True)
    db["user_recommendations"].create_index("user_id", unique=True)
    db["moderation_queue"].create_index("review_id", unique=True)

    client.close()
    print("Data generation complete.")


if __name__ == "__main__":
    generate_all()
