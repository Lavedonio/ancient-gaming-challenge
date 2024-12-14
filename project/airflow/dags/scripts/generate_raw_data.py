"""
This module contains the functions to create the mock data and do the cleanup afterwards.
"""
import os
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from random import randrange, choice, randint, sample
from pathlib import Path

import pandas as pd

DATA_LOCATION = Path(__file__).resolve().parent / 'random_data'

# Default values for testing
TODAY = datetime(2024, 12, 12)
NOW = datetime(2024, 12, 12, 6, 0, 0)


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
    event_timestamp: datetime

@dataclass
class RawTransaction():
    id: int
    user_id: int
    transaction_date: date
    amount: float
    type: str


class DataCreation:
    """This class contain extra logic to handle the data creation."""

    def __init__(self, execution_date: date, execution_datetime: datetime, id_: int, name: str, available_languages: list[str]):
        self.execution_date = execution_date
        self.execution_datetime = execution_datetime

        self.user_id = id_
        self.user_name = name
        self.available_languages = available_languages

        self.user = None
        self.user_preference = None
        self.number_of_transactions = None
        self.transactions = []

    def __generate_base_global_id(self, base_id: int):
        return int(self.execution_date.strftime('%Y%m%d')) * 100 + base_id

    def __generate_previous_base_global_id(self, base_id: int, days_ago: int = 0):
        return int((self.execution_date - timedelta(days=days_ago)).strftime('%Y%m%d')) * 100 + base_id

    @staticmethod
    def __generate_random_datetime(min_date: datetime, max_date: datetime):
        delta = max_date - min_date
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return min_date + timedelta(seconds=random_second)

    def generate_user_datapoints(self):
        self.user_email = self.user_name.lower().replace(' ', '.') + "@example.com"
        signup_datetime = self.__generate_random_datetime(datetime(2024, 1, 1), self.execution_datetime)
        self.user_registration_date = signup_datetime.date()

        self.user = RawUser(
            id=self.__generate_base_global_id(self.user_id),
            name=self.user_name,
            registration_date=self.execution_date,
            email=self.user_name.lower().replace(' ', '.') + "@example.com"
        )

        self.user_preference = RawUserPreference(
            id=self.__generate_base_global_id(self.user_id),
            user_id=self.__generate_base_global_id(self.user_id),
            preferred_language=choice(self.available_languages),
            notifications_enabled=choice([True, False]),
            marketing_opt_in=choice([True, False]),
            event_timestamp=self.execution_datetime - timedelta(seconds=randint(0, 3 * 60 * 60))
        )
        self.number_of_transactions = randrange(0, 4)
    
    def __transaction_logic(self, daily_transaction_counter: int, number_of_transactions: int, days_ago: int = 0) -> int:
        for transaction_number in range(1, number_of_transactions + 1):

            # Transaction Amount
            transaction_type = choice(['deposit', 'withdrawal'])
            multiplier = 1 if transaction_type == 'deposit' else -1

            amount = multiplier * randint(1, 10000) / 100

            if days_ago == 0:
                transaction = RawTransaction(
                    id=self.__generate_base_global_id(daily_transaction_counter),
                    user_id=self.user.id,
                    transaction_date=self.execution_date,
                    amount=amount,
                    type=transaction_type
                )
            else:
                transaction = RawTransaction(
                    id=self.__generate_base_global_id(daily_transaction_counter),
                    user_id=self.__generate_previous_base_global_id(transaction_number, days_ago),
                    transaction_date=self.execution_date - timedelta(days=days_ago),
                    amount=amount,
                    type=transaction_type
                )

            self.transactions.append(transaction)

            daily_transaction_counter += 1
        return daily_transaction_counter

    def generate_transactions(self, daily_transaction_counter: int) -> int:
        # Transactions for accounts created today
        daily_transaction_counter = self.__transaction_logic(daily_transaction_counter, self.number_of_transactions, 0)

        # Transactions for accounts created on previous days
        daily_transaction_counter = self.__transaction_logic(daily_transaction_counter, randrange(1, 5), 1)
        daily_transaction_counter = self.__transaction_logic(daily_transaction_counter, randrange(1, 5), 2)

        return daily_transaction_counter

    def __str__(self):
        user_info = "\n".join([
            f"{self.user}",
            f"{self.user_preference}",
            f"Transactions: {self.transactions}",
            f"{self.number_of_transactions=}",
        ])
        return user_info


def generate_raw_data(save_locally: bool = False, **kwargs):
    try:
        ds: str = kwargs['ds']
        ts: str = kwargs['ts']
    except KeyError:
        execution_date = TODAY
        execution_datetime = NOW
    else:
        execution_date = datetime.strptime(ds, '%Y-%m-%d').date()
        execution_datetime = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=None)

    daily_transaction_counter = 1

    with open(DATA_LOCATION / 'random_names.txt') as names_file:
        names = [x.replace('\n', '') for x in names_file.readlines()]

    with open(DATA_LOCATION / 'languages.txt') as lang_file:
        available_languages = [x.replace('\n', '') for x in lang_file.readlines()]

    number_of_new_users = randrange(5, 50)
    sampled_names = sample(names, number_of_new_users)

    datapoints: list[DataCreation] = []
    for index, name in enumerate(sampled_names, start=1):
        user_data = DataCreation(execution_date, execution_datetime, index, name, available_languages)
        user_data.generate_user_datapoints()
        daily_transaction_counter = user_data.generate_transactions(daily_transaction_counter)
        # print(user_data)
        # print()
        datapoints.append(user_data)

    users_df = pd.DataFrame([asdict(x.user) for x in datapoints])
    users_df.set_index('id', inplace=True)
    print("Users Dataframe:")
    print(users_df)

    user_preferences_df = pd.DataFrame([asdict(x.user_preference) for x in datapoints])
    user_preferences_df.set_index('id', inplace=True)
    print("Users Preferences Dataframe:")
    print(user_preferences_df)

    all_transactions = []
    for user_group in datapoints:
        for transaction in user_group.transactions:
            all_transactions.append(transaction)

    transactions_df = pd.DataFrame([asdict(x) for x in all_transactions])
    transactions_df.set_index('id', inplace=True)
    print("Transactions Dataframe:")
    print(transactions_df)

    if save_locally:
        users_df.to_csv('raw_users.csv')
        user_preferences_df.to_csv('raw_user_preferences.csv')
        transactions_df.to_csv('raw_transactions.csv')
    else:
        return (users_df, user_preferences_df, transactions_df)


def remove_files(files: list[str] | None = None, **kwargs):
    """
    Removes files generated by the generate_raw_data function.
    """
    if not files:
        files = ['raw_users.csv', 'raw_user_preferences.csv', 'raw_transactions.csv']

    for file in files:
        try:
            os.remove(file)
        except OSError:
            print(f"File {file} doesn't exist!")


if __name__ == '__main__':
    generate_raw_data(save_locally=True)
