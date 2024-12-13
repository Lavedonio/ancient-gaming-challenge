"""

"""
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from random import randrange, choice,randint
from pathlib import Path

import pandas as pd

DATA_LOCATION = Path(__file__).resolve().parent / 'random_data'
TODAY = datetime(2024, 12, 12)
NOW = datetime(2024, 12, 12, 6, 0, 0)
TRANSACTION_COUNTER = 0  # global var

@dataclass
class RawUser():
    id: int
    name: str
    registration_date: date
    email: str


@dataclass
class RawUserPreference():
    id: int
    user_id: int
    preferred_language: str
    notifications_enabled: bool
    marketing_opt_in: bool
    timestamp: datetime

@dataclass
class Transaction():
    id: int
    user_id: int
    transaction_date: date
    amount: float
    type: str


class DataCreation:
    """This class contain extra logic to handle the data creation."""

    def __init__(self, id_: int, name: str, available_languages: list[str]):
        self.user_id = id_
        self.user_name = name
        self.available_languages = available_languages

        self.user = None
        self.user_preference = None
        self.number_of_transactions = None
        self.transactions = []

    @staticmethod
    def __generate_random_date(min_date: date, max_date: date):
        delta_days = (max_date - min_date).days
        delta = max(1, delta_days)
        return min_date + timedelta(days=randrange(delta))

    @staticmethod
    def __generate_base_global_id(base_id: int):
        return int(datetime.now().strftime('%Y%m%d%H%M%S')) * 100

    @staticmethod
    def __generate_random_datetime(min_date: datetime, max_date: datetime):
        delta = max_date - min_date
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return min_date + timedelta(seconds=random_second)

    def generate_user_datapoints(self):
        self.user_email = self.user_name.lower().replace(' ', '.') + "@example.com"
        signup_datetime = self.__generate_random_datetime(datetime(2024, 1, 1), TODAY)
        self.user_registration_date = signup_datetime.date()

        self.user = RawUser(
            id=self.__generate_base_global_id(self.user_id),
            user_name=self.user_name,
            registration_date=TODAY,
            email=self.user_name.lower().replace(' ', '.') + "@example.com"
        )

        self.user_preference = RawUserPreference(
            id=self.__generate_base_global_id(self.user_id),
            user_id=self.__generate_base_global_id(self.user_id),
            preferred_language=choice(self.available_languages),
            notifications_enabled=choice([True, False]),
            marketing_opt_in=choice([True, False]),
            timestamp=NOW - timedelta(seconds=randint(0, 3 * 60 * 60))
        )
        self.number_of_transactions = randrange(0, 4)

    def generate_transactions(self):
        global TRANSACTION_COUNTER
        total_amount = 0

        for transaction_number in range(self.number_of_transactions):

            # Transaction Amount
            transaction_type = choice(['deposit', 'withdrawal'])
            multiplier = 1 if transaction_type == 'deposit' else -1

            amount = multiplier * randint(1, 10000) / 100

            if total_amount + amount < 0:
                amount = total_amount
            
            total_amount += amount

            # Transaction Date
            transaction_date = self.__generate_random_date(
                min_date=last_transaction_date + timedelta(days=1),
                max_date=TODAY.date() - timedelta(days=2 * (self.number_of_transactions - transaction_number))
            )
            last_transaction_date = transaction_date

            # Transaction ID
            TRANSACTION_COUNTER += 1

            self.transactions.append(
                Transaction(
                    id=TRANSACTION_COUNTER,
                    user_id=self.user.id,
                    transaction_date=transaction_date,
                    amount=amount,
                    type=TODAY
                )
            )

    def __str__(self):
        user_info = "\n".join([
            f"{self.user}",
            f"{self.user_preference}",
            f"Transactions: {self.transactions}",
            f"{self.number_of_transactions=}",
        ])
        return user_info


def generate_raw_data():
    with open(DATA_LOCATION / 'random_names.txt') as names_file:
        names = [x.replace('\n', '') for x in names_file.readlines()]

    with open(DATA_LOCATION / 'languages.txt') as lang_file:
        available_languages = [x.replace('\n', '') for x in lang_file.readlines()]

    datapoints: list[DataCreation] = []
    for index, name in enumerate(names, start=1):
        user_data = DataCreation(index, name, available_languages)
        user_data.generate_user_datapoints()
        user_data.generate_transactions()
        # print(user_data)
        # print()
        datapoints.append(user_data)

    users_df = pd.DataFrame([asdict(x.user) for x in datapoints])
    users_df.set_index('id', inplace=True)
    print(users_df)
    users_df.to_csv('users.csv')

    user_preferences_df = pd.DataFrame([asdict(x.user_preference) for x in datapoints])
    user_preferences_df.set_index('id', inplace=True)
    print(user_preferences_df)
    user_preferences_df.to_csv('user_preferences.csv')

    all_transactions = []
    for user_group in datapoints:
        for transaction in user_group.transactions:
            all_transactions.append(transaction)

    transactions_df = pd.DataFrame([asdict(x) for x in all_transactions])
    transactions_df.set_index('id', inplace=True)
    print(transactions_df)
    transactions_df.to_csv('transactions.csv')


if __name__ == '__main__':
    generate_raw_data()
