"""
Seed Test Data Generator
=========================
Generates realistic sample transaction data for testing and demos.
Creates CSV files that can be loaded into Snowflake via the pipeline.

Usage:
    python scripts/seed_test_data.py --records 10000
    python scripts/seed_test_data.py --records 500000 --output data/
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path


# --- Configuration ---
CUSTOMERS = [f"CUST_{i:04d}" for i in range(1, 201)]
MERCHANTS = [f"MERCH_{i:03d}" for i in range(1, 51)]

CURRENCIES = ["USD", "USD", "USD", "USD", "EUR", "GBP", "CAD", "AUD", "JPY", "INR"]
STATUSES = ["approved", "approved", "approved", "approved", "approved",
            "declined", "pending", "reversed", "settled"]
CHANNELS = ["online", "online", "in_store", "in_store", "mobile", "atm", "phone"]
CARD_TYPES = ["visa", "visa", "mastercard", "mastercard", "amex", "discover"]

MERCHANT_CATEGORIES = [
    "grocery", "retail", "restaurant", "gas", "travel",
    "entertainment", "healthcare", "utilities", "streaming",
    "dining", "hotel", "airline", "clothing", "pharmacy",
]

CITIES = {
    "US": ["New York", "Chicago", "San Francisco", "Los Angeles", "Miami",
           "Houston", "Boston", "Seattle", "Portland", "Denver"],
    "GB": ["London", "Manchester", "Birmingham"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "DE": ["Berlin", "Munich"],
    "JP": ["Tokyo", "Osaka"],
    "AU": ["Sydney", "Melbourne"],
    "IN": ["Mumbai", "Delhi", "Bangalore"],
}

RESPONSE_CODES = {
    "approved": "00",
    "declined": ["05", "14", "51", "54"],
    "pending": "00",
    "reversed": "00",
    "settled": "00",
}

CUSTOMER_TIERS = ["standard", "standard", "standard", "gold", "gold", "platinum"]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
    "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
    "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    "Priya", "Raj", "Wei", "Yuki", "Mohammed", "Fatima",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor",
    "Patel", "Kumar", "Chen", "Wang", "Kim", "Nakamura",
]


def generate_transaction(record_id: int, base_time: datetime) -> dict:
    """Generate a single realistic transaction record."""
    customer = random.choice(CUSTOMERS)
    merchant = random.choice(MERCHANTS)
    status = random.choice(STATUSES)
    country = random.choices(
        list(CITIES.keys()),
        weights=[50, 10, 8, 5, 5, 5, 10],  # US-weighted
        k=1,
    )[0]
    city = random.choice(CITIES[country])

    # Amount distribution: most transactions are small
    amount_type = random.random()
    if amount_type < 0.5:
        amount = round(random.uniform(5.00, 50.00), 2)
    elif amount_type < 0.8:
        amount = round(random.uniform(50.00, 200.00), 2)
    elif amount_type < 0.95:
        amount = round(random.uniform(200.00, 1000.00), 2)
    else:
        amount = round(random.uniform(1000.00, 10000.00), 2)

    # Timestamp: spread across recent hours
    offset_minutes = random.randint(0, 1440)  # Last 24 hours
    timestamp = base_time - timedelta(minutes=offset_minutes)

    card_type = random.choice(CARD_TYPES)
    card_last_four = f"{random.randint(1000, 9999)}"

    response_code = RESPONSE_CODES.get(status, "00")
    if isinstance(response_code, list):
        response_code = random.choice(response_code)

    return {
        "transaction_id": f"TXN_{uuid.uuid4().hex[:12].upper()}",
        "customer_id": customer,
        "merchant_id": merchant,
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "transaction_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        "status": status,
        "channel": random.choice(CHANNELS),
        "card_type": card_type,
        "card_number": f"****{card_last_four}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "merchant_city": city,
        "merchant_country": country,
        "response_code": response_code,
        "auth_code": f"AUTH{random.randint(100000, 999999)}" if status == "approved" else "",
    }


def generate_customer(customer_id: str) -> dict:
    """Generate a customer master record."""
    country = random.choices(
        list(CITIES.keys()),
        weights=[50, 10, 8, 5, 5, 5, 10],
        k=1,
    )[0]
    city = random.choice(CITIES[country])

    return {
        "customer_id": customer_id,
        "first_name": random.choice(FIRST_NAMES),
        "last_name": random.choice(LAST_NAMES),
        "email": f"{customer_id.lower()}@example.com",
        "phone": f"+1{random.randint(2000000000, 9999999999)}",
        "address_line_1": f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Park', 'Lake'])} St",
        "city": city,
        "state": "",
        "country": country,
        "postal_code": f"{random.randint(10000, 99999)}",
        "customer_tier": random.choice(CUSTOMER_TIERS),
        "account_open_date": (
            datetime.now() - timedelta(days=random.randint(30, 1825))
        ).strftime("%Y-%m-%d"),
    }


def write_transactions(records: int, output_dir: str) -> str:
    """Generate and write transaction CSV."""
    output_path = os.path.join(output_dir, "sample_transactions.csv")
    base_time = datetime.now()

    fieldnames = [
        "transaction_id", "customer_id", "merchant_id", "amount",
        "currency", "transaction_timestamp", "status", "channel",
        "card_type", "card_number", "merchant_category", "merchant_city",
        "merchant_country", "response_code", "auth_code",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(records):
            row = generate_transaction(i, base_time)
            writer.writerow(row)

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i + 1:,}/{records:,} transactions...")

    print(f"[OK] Wrote {records:,} transactions to {output_path}")
    return output_path


def write_customers(output_dir: str) -> str:
    """Generate and write customer master CSV."""
    output_path = os.path.join(output_dir, "sample_customers.csv")

    fieldnames = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "address_line_1", "city", "state", "country", "postal_code",
        "customer_tier", "account_open_date",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for customer_id in CUSTOMERS:
            writer.writerow(generate_customer(customer_id))

    print(f"[OK] Wrote {len(CUSTOMERS)} customers to {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate sample transaction data")
    parser.add_argument(
        "--records", type=int, default=10000,
        help="Number of transaction records to generate (default: 10000)",
    )
    parser.add_argument(
        "--output", type=str, default="tests/fixtures",
        help="Output directory (default: tests/fixtures)",
    )
    args = parser.parse_args()

    # Create output directory
    Path(args.output).mkdir(parents=True, exist_ok=True)

    print("=" * 50)
    print("Transaction Data Pipeline - Test Data Generator")
    print("=" * 50)
    print(f"Records:    {args.records:,}")
    print(f"Output:     {args.output}")
    print()

    # Generate data
    write_transactions(args.records, args.output)
    write_customers(args.output)

    print()
    print("Test data generation complete!")
    print("   Load into Snowflake with: COPY INTO ... FROM @stage")


if __name__ == "__main__":
    main()
