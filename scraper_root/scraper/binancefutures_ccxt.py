import threading
import time
import logging
from typing import List
import datetime
import ccxt

from scraper_root.scraper.data_classes import (
    AssetBalance,
    Position,
    Tick,
    Balance,
    Income,
    Order,
    Account,
)
from scraper_root.scraper.persistence.repository import Repository

logger = logging.getLogger()


def is_asset_usd_or_derivative(asset: str):
    return asset.lower() in ["usdt", "busd", "usd", "usdc"]


class BinanceFuturesCCXT:
    def __init__(
        self,
        account: Account,
        symbols: List[str],
        repository: Repository,
    ):
        print("Binance initialized")
        self.account = account
        self.alias = self.account.alias
        self.symbols = symbols
        self.api_key = self.account.api_key
        self.secret = self.account.api_secret
        self.repository = repository

        # Initialize ccxt Binance client
        self.exchange = ccxt.binanceusdm(
            {"apiKey": self.api_key, "secret": self.secret, "enableRateLimit": True}
        )
        self.exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

    def start(self):
        logger.info(f"{self.alias}: Starting binance futures scraper")

        for symbol in self.symbols:
            symbol_trade_thread = threading.Thread(
                name=f"trade_thread_{symbol}",
                target=self.process_trades,
                args=(symbol,),
                daemon=True,
            )
            symbol_trade_thread.start()

        sync_balance_thread = threading.Thread(
            name=f"sync_balance_thread", target=self.sync_account, daemon=True
        )
        sync_balance_thread.start()

        sync_trades_thread = threading.Thread(
            name=f"sync_trades_thread", target=self.sync_trades, daemon=True
        )
        sync_trades_thread.start()

        sync_orders_thread = threading.Thread(
            name=f"sync_orders_thread", target=self.sync_open_orders, daemon=True
        )
        sync_orders_thread.start()

    def sync_trades(self):
        max_fetches_in_cycle = 3
        first_trade_reached = False
        while True:
            try:
                counter = 0
                while first_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    oldest_income = self.repository.get_oldest_income(
                        account=self.account.alias
                    )
                    if oldest_income is None:
                        oldest_timestamp = int(
                            datetime.datetime.now(datetime.timezone.utc).timestamp()
                            * 1000
                        )
                    else:
                        oldest_timestamp = oldest_income.timestamp
                        logger.info(f"{self.alias}: Synced trades before {oldest_timestamp}")

                    exchange_incomes = self.exchange.fapiprivate_get_income(
                        {"limit": 1000, "endTime": oldest_timestamp - 1}
                    )
                    logger.info(
                        f"{self.alias}: Length of older trades fetched up to {oldest_timestamp}: {len(exchange_incomes)}"
                    )
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income["asset"]):
                            exchange_income["income"] = self.income_to_usdt(
                                float(exchange_income["income"]),
                                int(exchange_income["time"]),
                                exchange_income["asset"],
                            )
                            exchange_income["asset"] = "USDT"

                        income = Income(
                            symbol=exchange_income["symbol"],
                            asset=exchange_income["asset"],
                            type=exchange_income["incomeType"],
                            income=float(exchange_income["income"]),
                            timestamp=exchange_income["time"],
                            transaction_id=exchange_income["tranId"],
                        )
                        incomes.append(income)
                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(exchange_incomes) < 1:
                        first_trade_reached = True
                    time.sleep(150)

                newest_trade_reached = False
                while newest_trade_reached is False and counter < max_fetches_in_cycle:
                    counter += 1
                    newest_income = self.repository.get_newest_income(
                        account=self.account.alias
                    )
                    if newest_income is None:
                        newest_timestamp = int(
                            datetime.datetime.fromisoformat(
                                "2017-09-01 00:00:00+00:00"
                            ).timestamp()
                            * 1000
                        )
                    else:
                        newest_timestamp = newest_income.timestamp
                        logger.info(f"{self.alias}: Synced newer trades since {newest_timestamp}")

                    exchange_incomes = self.exchange.fapiprivate_get_income(
                        {"limit": 1000, "startTime": newest_timestamp + 1}
                    )
                    logger.info(
                        f"{self.alias}: Length of newer trades fetched from {newest_timestamp}: {len(exchange_incomes)}"
                    )
                    incomes = []
                    for exchange_income in exchange_incomes:
                        if not is_asset_usd_or_derivative(exchange_income["asset"]):
                            exchange_income["income"] = self.income_to_usdt(
                                float(exchange_income["income"]),
                                int(exchange_income["time"]),
                                exchange_income["asset"],
                            )
                            exchange_income["asset"] = "USDT"

                        income = Income(
                            symbol=exchange_income["symbol"],
                            asset=exchange_income["asset"],
                            type=exchange_income["incomeType"],
                            income=float(exchange_income["income"]),
                            timestamp=exchange_income["time"],
                            transaction_id=exchange_income["tranId"],
                        )
                        incomes.append(income)
                    self.repository.process_incomes(incomes, account=self.account.alias)
                    if len(exchange_incomes) < 1:
                        newest_trade_reached = True

                logger.info(f"{self.alias}: Synced trades")
            except Exception as e:
                logger.error(f"{self.alias}: Failed to process trades: {e}")

            time.sleep(150)

    def income_to_usdt(self, income: float, income_timestamp: int, asset: str) -> float:
        if is_asset_usd_or_derivative(asset):
            return income

        symbol = f"{asset}:USDT"  # symbol format for fetching candles
        candles = self.exchange.fetch_ohlcv(
            symbol, timeframe="1m", since=income_timestamp - 1000, limit=1
        )

        close_price = candles[-1][4]
        income *= close_price

        return income

    def sync_account(self):
        while True:
            try:
                account = self.exchange.fapiprivatev2_get_account()
                asset_balances = [
                    AssetBalance(
                        asset=asset["asset"],
                        balance=float(asset["walletBalance"]),
                        unrealizedProfit=float(asset["unrealizedProfit"]),
                    )
                    for asset in account["assets"]
                ]

                usd_assets = [
                    asset
                    for asset in account["assets"]
                    if asset["asset"] in ["BUSD", "USDT", "USDC"]
                ]
                total_wallet_balance = sum(
                    [float(asset["walletBalance"]) for asset in usd_assets]
                )
                total_upnl = sum(
                    [float(asset["unrealizedProfit"]) for asset in usd_assets]
                )

                logger.info(
                    f"{self.alias}: Wallet balance: {total_wallet_balance}, upnl: {total_upnl}"
                )

                balance = Balance(
                    totalBalance=total_wallet_balance,
                    totalUnrealizedProfit=total_upnl,
                    assets=asset_balances,
                )
                self.repository.process_balances(balance, account=self.account.alias)

                positions = [
                    Position(
                        symbol=position["symbol"],
                        entry_price=float(position["entryPrice"]),
                        position_size=float(position["positionAmt"]),
                        side=position["positionSide"],
                        unrealizedProfit=float(position["unrealizedProfit"]),
                        initial_margin=float(position["initialMargin"]),
                    )
                    for position in account["positions"]
                    if position["positionSide"] != "BOTH"
                ]
                self.repository.process_positions(positions, account=self.account.alias)

                for position in positions:
                    if position.position_size != 0.0:
                        symbol = position.symbol
                        retrieved_tick = self.exchange.fetch_ticker(symbol)
                        tick = Tick(
                            symbol=symbol.upper(),
                            price=float(retrieved_tick["last"]),
                            qty=float(retrieved_tick["vwap"]),
                            timestamp=int(retrieved_tick["timestamp"]),
                        )
                        self.repository.process_tick(tick, account=self.account.alias)
                        logger.debug(f"{self.alias}: Synced recent trade price for {symbol}")

                logger.info(f"{self.alias}: Synced account")
            except Exception as e:
                logger.error(f"{self.alias}: Failed to process balance: {e}")
            time.sleep(150)

    def sync_open_orders(self):
        while True:
            orders = []
            try:
                open_orders = self.exchange.fetch_open_orders()
                for open_order in open_orders:
                    order = Order()
                    order.symbol = (
                        open_order["symbol"]
                        .upper()
                        .replace("/", "")
                        .replace(":USDT", "")
                    )
                    order.price = float(open_order["price"])
                    order.quantity = float(open_order["amount"])
                    order.side = open_order["side"].upper()
                    order.position_side = open_order["info"]["positionSide"].upper()
                    order.type = open_order["type"].capitalize()
                    orders.append(order)
                self.repository.process_orders(orders, account=self.account.alias)
                logger.info(f"{self.alias}: Synced orders")

            except Exception as e:
                logger.error(
                    f"{self.alias}: Failed to process open orders for symbol: {e}"
                )

            time.sleep(150)

    def process_trades(self, symbol: str):
        while True:
            try:
                retrieved_tick = self.exchange.fetch_ticker(symbol)
                tick = Tick(
                    symbol=symbol.upper(),
                    price=float(retrieved_tick["last"]),
                    qty=float(retrieved_tick["vwap"]),
                    timestamp=int(retrieved_tick["timestamp"]),
                )
                self.repository.process_tick(tick, account=self.account.alias)
            except Exception as e:
                logger.warning(f"{self.alias}: Error processing tick: {e}")
            time.sleep(150)
