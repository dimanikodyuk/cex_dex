import asyncio
import csv
import os
import time
from datetime import datetime
from typing import Optional, Tuple, Dict
import logging
from collections import deque

import aiohttp
from dotenv import load_dotenv

# Завантаження конфігурації
load_dotenv()

# Конфігурація
BYBIT_SYMBOL = os.getenv('BYBIT_SYMBOL', 'BTCUSDT')
MIN_SPREAD = float(os.getenv('MIN_SPREAD', 0.3))
GAS_LIMIT_BNB = float(os.getenv('GAS_LIMIT_BNB', 0.0005))
BNB_PRICE_ORACLE = float(os.getenv('BNB_PRICE_ORACLE', 300))
DEX_FEE = float(os.getenv('DEX_FEE', 0.0025))  # PancakeSwap V3 fee 0.25%

# Telegram конфігурація
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Клас для відправки сповіщень в Telegram"""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_notification_time = 0
        self.min_notification_interval = 60  # Мінімум 60 секунд між сповіщеннями

    async def _ensure_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """Відправка повідомлення в Telegram"""
        if not self.bot_token or not self.chat_id:
            return False

        # Перевірка на спам (не частіше ніж раз в 60 секунд)
        current_time = time.time()
        if current_time - self.last_notification_time < self.min_notification_interval:
            logger.debug("Пропущено сповіщення (занадто часто)")
            return False

        await self._ensure_session()

        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': parse_mode,
                'disable_web_page_preview': True
            }

            async with self.session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    self.last_notification_time = current_time
                    logger.info("Telegram сповіщення відправлено")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Помилка Telegram API: {response.status} - {error_text}")
                    return False

        except asyncio.TimeoutError:
            logger.error("Таймаут при відправці Telegram сповіщення")
            return False
        except Exception as e:
            logger.error(f"Помилка відправки Telegram сповіщення: {e}")
            return False

    async def send_arbitrage_alert(self, direction: str, spread: float, profit: float,
                                   cex_price: float, dex_price: float, costs: float):
        """Відправка сповіщення про арбітражну можливість"""

        direction_emoji = "🟢" if direction == "CEX→DEX" else "🔵"
        direction_text = f"{direction_emoji} {direction}"

        if direction == "CEX→DEX":
            action = "Купити на ByBit → Продати на PancakeSwap"
            price_diff = dex_price - cex_price
        else:
            action = "Купити на PancakeSwap → Продати на ByBit"
            price_diff = cex_price - dex_price

        message = f"""
🚨 <b>АРБІТРАЖНА МОЖЛИВІСТЬ!</b> 🚨

{direction_text} <b>{direction}</b>

📊 <b>Стратегія:</b> {action}
💰 <b>Прибуток:</b> <code>${profit:,.2f}</code>
📈 <b>Спред:</b> <code>{spread:+.2f}%</code>

🏦 <b>ByBit (CEX):</b> <code>${cex_price:,.2f}</code>
🥞 <b>PancakeSwap (DEX):</b> <code>${dex_price:,.2f}</code>
📉 <b>Різниця:</b> <code>${price_diff:+.2f}</code>

💸 <b>Витрати:</b>
   • Комісія CEX: <code>${cex_price * 0.001:,.2f}</code>
   • Комісія DEX: <code>${dex_price * 0.0025:,.2f}</code>
   • Газ BNB: <code>${costs - (cex_price * 0.001 + dex_price * 0.0025):,.2f}</code>

⏰ <b>Час:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<i>#Arbitrage #BTC #ByBit #PancakeSwap</i>
"""
        await self.send_message(message)

    async def send_status_message(self, message: str):
        """Відправка статусного повідомлення"""
        await self.send_message(message)

    async def close(self):
        if self.session:
            await self.session.close()


class ByBitMonitor:
    """Монітор цін ByBit через WebSocket"""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.current_price: Optional[float] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._reconnect_delay = 1
        self.price_history = deque(maxlen=10)

    async def connect(self):
        """Підключення до WebSocket ByBit"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

        logger.info(f"Підключення до ByBit WebSocket для {self.symbol}")
        self.ws = await self.session.ws_connect(self.ws_url)

        subscribe_msg = {
            "op": "subscribe",
            "args": [f"tickers.{self.symbol}"]
        }
        await self.ws.send_json(subscribe_msg)
        logger.info(f"Підписано на tickers.{self.symbol}")
        self._reconnect_delay = 1

    async def start(self):
        """Запуск моніторингу WebSocket"""
        self._running = True

        while self._running:
            try:
                await self.connect()

                async for msg in self.ws:
                    if not self._running:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        await self._handle_message(data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("WebSocket помилка")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.warning("WebSocket закрито")
                        break

            except Exception as e:
                logger.error(f"WebSocket розірвано: {e}")
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(self._reconnect_delay * 2, 30)

    async def _handle_message(self, data: dict):
        """Обробка вхідних повідомлень від ByBit"""
        try:
            if 'topic' in data and data['topic'] == f'tickers.{self.symbol}':
                ticker_data = data.get('data', {})
                if 'lastPrice' in ticker_data:
                    price = float(ticker_data['lastPrice'])
                    self.current_price = price
                    self.price_history.append(price)
                    logger.debug(f"ByBit ціна оновлена: ${price:,.2f}")
        except Exception as e:
            logger.error(f"Помилка обробки повідомлення ByBit: {e}")

    def get_price(self) -> Optional[float]:
        """Отримання поточної ціни"""
        return self.current_price

    def get_smooth_price(self) -> Optional[float]:
        """Отримання згладженої ціни"""
        if len(self.price_history) > 0:
            return sum(self.price_history) / len(self.price_history)
        return self.current_price

    async def stop(self):
        """Зупинка моніторингу"""
        self._running = False
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()


class PancakeSwapAPIMonitor:
    """Монітор цін через API з кешуванням"""

    def __init__(self):
        self.current_price: Optional[float] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_update_time = 0
        self.update_interval = 3
        self.request_lock = asyncio.Lock()

    async def get_price(self) -> Optional[float]:
        """
        Отримання ціни BTC/USDT
        """
        current_time = time.time()

        # Повертаємо кешовану ціну
        if self.current_price and (current_time - self.last_update_time) < self.update_interval:
            return self.current_price

        async with self.request_lock:
            # Подвійна перевірка після отримання блокування
            if self.current_price and (current_time - self.last_update_time) < self.update_interval:
                return self.current_price

            try:
                if not self.session:
                    self.session = aiohttp.ClientSession()

                # Використовуємо Binance API як основний
                url = "https://api.binance.com/api/v3/ticker/price"
                params = {"symbol": "BTCUSDT"}

                async with self.session.get(url, params=params, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = float(data["price"])
                        self.current_price = price
                        self.last_update_time = current_time
                        logger.debug(f"DEX ціна оновлена: ${price:,.2f}")
                        return price
                    else:
                        logger.warning(f"API повернув статус {response.status}")
                        return self.current_price

            except asyncio.TimeoutError:
                logger.warning("Таймаут API, використовую кешовану ціну")
                return self.current_price
            except Exception as e:
                logger.error(f"Помилка отримання ціни: {e}")
                return self.current_price

    async def close(self):
        if self.session:
            await self.session.close()


class ArbitrageCalculator:
    """Розрахунок арбітражного спреду (двосторонній)"""

    def __init__(self, cex_fee: float = 0.001, dex_fee: float = 0.0025,
                 gas_bnb: float = 0.0005, bnb_price: float = 300):
        self.cex_fee = cex_fee
        self.dex_fee = dex_fee
        self.gas_bnb = gas_bnb
        self.bnb_price = bnb_price

    def calculate_both_directions(self, price_cex: float, price_dex: float) -> Dict:
        """
        Розрахунок арбітражу в обидві сторони
        Повертає словник з результатами для кожного напрямку
        """
        if price_cex == 0 or price_dex == 0:
            return {
                'cex_to_dex': {
                    'direction': 'CEX→DEX',
                    'spread': 0,
                    'profit': 0,
                    'costs': 0,
                    'action': 'Buy CEX → Sell DEX'
                },
                'dex_to_cex': {
                    'direction': 'DEX→CEX',
                    'spread': 0,
                    'profit': 0,
                    'costs': 0,
                    'action': 'Buy DEX → Sell CEX'
                }
            }

        gas_cost_usd = self.gas_bnb * self.bnb_price

        # 1. Арбітраж: ByBit → PancakeSwap (купити на CEX, продати на DEX)
        cex_buy_fee = price_cex * self.cex_fee
        dex_sell_fee = price_dex * self.dex_fee
        total_costs_cex_to_dex = cex_buy_fee + dex_sell_fee + gas_cost_usd
        profit_cex_to_dex = (price_dex - price_cex) - total_costs_cex_to_dex
        spread_cex_to_dex = (profit_cex_to_dex / price_cex) * 100 if price_cex > 0 else 0

        # 2. Арбітраж: PancakeSwap → ByBit (купити на DEX, продати на CEX)
        dex_buy_fee = price_dex * self.dex_fee
        cex_sell_fee = price_cex * self.cex_fee
        total_costs_dex_to_cex = dex_buy_fee + cex_sell_fee + gas_cost_usd
        profit_dex_to_cex = (price_cex - price_dex) - total_costs_dex_to_cex
        spread_dex_to_cex = (profit_dex_to_cex / price_dex) * 100 if price_dex > 0 else 0

        return {
            'cex_to_dex': {
                'direction': 'CEX→DEX',
                'spread': spread_cex_to_dex,
                'profit': profit_cex_to_dex,
                'costs': total_costs_cex_to_dex,
                'action': 'Buy on ByBit → Sell on PancakeSwap'
            },
            'dex_to_cex': {
                'direction': 'DEX→CEX',
                'spread': spread_dex_to_cex,
                'profit': profit_dex_to_cex,
                'costs': total_costs_dex_to_cex,
                'action': 'Buy on PancakeSwap → Sell on ByBit'
            }
        }

    def get_best_opportunity(self, price_cex: float, price_dex: float, min_spread: float) -> Optional[Dict]:
        """Повертає найкращу арбітражну можливість"""
        results = self.calculate_both_directions(price_cex, price_dex)

        best = None
        best_profit = -float('inf')

        for direction in ['cex_to_dex', 'dex_to_cex']:
            result = results[direction]
            if result['profit'] > best_profit and result['profit'] > 0 and result['spread'] > min_spread:
                best_profit = result['profit']
                best = result

        return best


class ArbitrageMonitor:
    """Головний клас моніторингу арбітражу"""

    def __init__(self):
        self.bybit = ByBitMonitor(os.getenv('BYBIT_SYMBOL', 'BTCUSDT'))
        self.pancake = PancakeSwapAPIMonitor()
        self.calculator = ArbitrageCalculator(
            cex_fee=0.001,
            dex_fee=DEX_FEE,
            gas_bnb=GAS_LIMIT_BNB,
            bnb_price=BNB_PRICE_ORACLE
        )
        self.telegram = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) if TELEGRAM_ENABLED else None

        self.csv_file = 'arbitrage_log.csv'
        self.opportunities_found = 0
        self._init_csv()

        # Для запобігання дублюванню сповіщень
        self.last_notified_direction = ""
        self.last_notified_profit = 0
        self.last_notified_time = 0
        self.notification_cooldown = 300  # 5 хвилин

        self.stats = {
            'checks': 0,
            'profitable_cex_to_dex': 0,
            'profitable_dex_to_cex': 0,
            'max_spread': -999,
            'min_spread': 999,
            'max_profit': 0
        }

    def _init_csv(self):
        """Ініціалізація CSV файлу"""
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'direction', 'action', 'cex_price', 'dex_price',
                    'spread_percent', 'net_profit_usd', 'total_costs_usd'
                ])

    async def log_opportunity(self, data: dict):
        """Логування арбітражної можливості в CSV"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    data['timestamp'],
                    data['direction'],
                    data['action'],
                    data['cex_price'],
                    data['dex_price'],
                    round(data['spread_percent'], 4),
                    round(data['net_profit_usd'], 4),
                    round(data['total_costs_usd'], 4)
                ])
            self.opportunities_found += 1
        except Exception as e:
            logger.error(f"Помилка запису в CSV: {e}")

    async def send_telegram_alert(self, opportunity: dict, cex_price: float, dex_price: float):
        """Відправка сповіщення в Telegram з перевіркою на дублювання"""
        if not self.telegram:
            return

        current_time = time.time()

        # Перевірка чи не надсилали сповіщення за останні 5 хвилин
        if current_time - self.last_notified_time < self.notification_cooldown:
            logger.info(f"Пропущено Telegram сповіщення (cooldown {self.notification_cooldown}с)")
            return

        # Перевірка чи це та ж сама можливість
        if (opportunity['direction'] == self.last_notified_direction and
                abs(opportunity['profit'] - self.last_notified_profit) < 5):
            logger.info(f"Пропущено Telegram сповіщення (аналогічна можливість)")
            return

        # Відправка сповіщення
        await self.telegram.send_arbitrage_alert(
            opportunity['direction'],
            opportunity['spread'],
            opportunity['profit'],
            cex_price,
            dex_price,
            opportunity['costs']
        )

        self.last_notified_direction = opportunity['direction']
        self.last_notified_profit = opportunity['profit']
        self.last_notified_time = current_time

    def clear_screen(self):
        """Очищення екрану"""
        os.system('cls' if os.name == 'nt' else 'clear')

    async def monitor_loop(self):
        """Головний цикл моніторингу"""
        self.clear_screen()

        print("=" * 80)
        print("🚀 ЗАПУСК МОНІТОРИНГУ АРБІТРАЖУ ByBit ↔ PancakeSwap V3 (ДВОСТОРОННІЙ)")
        print("=" * 80)
        print(f"📊 Торгова пара: BTC/USDT")
        print(f"🎯 Мінімальний спред: {MIN_SPREAD}%")
        print(
            f"💸 Комісія CEX: 0.1% | Комісія DEX: {DEX_FEE * 100}% | Газ: {GAS_LIMIT_BNB} BNB (${GAS_LIMIT_BNB * BNB_PRICE_ORACLE:.2f})")
        print(f"\n📈 Напрямки арбітражу:")
        print(f"   • CEX → DEX: Купівля на ByBit, продаж на PancakeSwap")
        print(f"   • DEX → CEX: Купівля на PancakeSwap, продаж на ByBit")

        if TELEGRAM_ENABLED:
            print(f"\n📱 Telegram сповіщення: УВІМКНЕНО")
            await self.telegram.send_status_message(
                f"✅ <b>Моніторинг арбітражу запущено (ДВОСТОРОННІЙ)</b>\n\n"
                f"📊 Пара: BTC/USDT\n"
                f"🎯 Мін. спред: {MIN_SPREAD}%\n"
                f"💸 Комісії: CEX 0.1% | DEX {DEX_FEE * 100}%\n"
                f"⏰ Час: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            print(f"\n📱 Telegram сповіщення: ВИМКНЕНО (додайте TELEGRAM_BOT_TOKEN та TELEGRAM_CHAT_ID в .env)")

        print("=" * 80)

        # Запуск WebSocket для ByBit
        bybit_task = asyncio.create_task(self.bybit.start())

        # Чекаємо перші дані
        await asyncio.sleep(3)

        last_dex_update = 0
        last_stats_update = time.time()

        try:
            while True:
                # Отримання ціни з ByBit
                cex_price = self.bybit.get_price()

                # Отримання ціни з DEX
                current_time = time.time()
                if current_time - last_dex_update >= 3:
                    dex_price = await self.pancake.get_price()
                    last_dex_update = current_time
                else:
                    dex_price = self.pancake.current_price

                # Показуємо статус підключення
                if cex_price is None:
                    print(f"\r⏳ [{datetime.now().strftime('%H:%M:%S')}] Очікування даних від ByBit...", end='',
                          flush=True)
                    await asyncio.sleep(0.5)
                    continue

                if dex_price is None:
                    print(f"\r⏳ [{datetime.now().strftime('%H:%M:%S')}] Очікування даних від PancakeSwap...", end='',
                          flush=True)
                    await asyncio.sleep(0.5)
                    continue

                if cex_price > 0 and dex_price > 0:
                    # Розрахунок обох напрямків
                    opportunities = self.calculator.calculate_both_directions(cex_price, dex_price)

                    timestamp = datetime.now().strftime('%H:%M:%S')
                    self.stats['checks'] += 1

                    # Оновлення статистики
                    for key in ['cex_to_dex', 'dex_to_cex']:
                        opp = opportunities[key]
                        self.stats['max_spread'] = max(self.stats['max_spread'], opp['spread'])
                        self.stats['min_spread'] = min(self.stats['min_spread'], opp['spread'])
                        self.stats['max_profit'] = max(self.stats['max_profit'], opp['profit'])

                    # Оновлення статистики кожні 10 секунд
                    if current_time - last_stats_update >= 10:
                        self.clear_screen()
                        last_stats_update = current_time

                    # Форматований вивід
                    print(f"\n{'═' * 80}")
                    print(f"⏰ {timestamp} | Перевірка #{self.stats['checks']}")
                    print(f"{'═' * 80}")
                    print(f"📈 ByBit (CEX):      ${cex_price:>12,.2f}")
                    print(f"🥞 PancakeSwap (DEX): ${dex_price:>12,.2f}")
                    print(f"📊 Різниця:          ${dex_price - cex_price:>+12,.2f}")
                    print(f"{'─' * 80}")

                    # Вивід обох напрямків
                    print(f"\n🎯 МОЖЛИВІ НАПРЯМКИ АРБІТРАЖУ:")

                    for direction in ['cex_to_dex', 'dex_to_cex']:
                        opp = opportunities[direction]
                        profit_color = "🟢" if opp['profit'] > 0 else "🔴"

                        print(f"\n  {opp['direction']}:")
                        print(f"     📝 {opp['action']}")
                        print(f"     💰 Прибуток:     ${opp['profit']:>+10,.2f} {profit_color}")
                        print(f"     📈 Спред:        {opp['spread']:>+9.2f}%")
                        print(f"     📉 Витрати:      ${opp['costs']:>10,.2f}")

                    # Перевірка обох напрямків на прибутковість
                    best_opportunity = None

                    for direction, opp in opportunities.items():
                        if opp['profit'] > 0 and opp['spread'] > MIN_SPREAD:
                            # Оновлення статистики
                            if direction == 'cex_to_dex':
                                self.stats['profitable_cex_to_dex'] += 1
                            else:
                                self.stats['profitable_dex_to_cex'] += 1

                            best_opportunity = opp

                            # Вивід знайденої можливості
                            print(f"\n{'🔥' * 40}")
                            print(f"  АРБІТРАЖНА МОЖЛИВІСТЬ! {opp['direction']}")
                            print(f"  {opp['action']}")
                            print(f"  Спред: {opp['spread']:.2f}% | Прибуток: ${opp['profit']:.2f}")
                            print(f"  Знайдено можливостей: {self.opportunities_found + 1}")
                            print(f"{'🔥' * 40}")

                            # Відправка Telegram сповіщення
                            if TELEGRAM_ENABLED:
                                await self.send_telegram_alert(opp, cex_price, dex_price)

                                # Звуковий сигнал
                                try:
                                    if os.name == 'nt':
                                        import winsound
                                        winsound.Beep(1000, 500)
                                    else:
                                        print('\a', end='', flush=True)
                                except:
                                    pass

                            await self.log_opportunity({
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'direction': opp['direction'],
                                'action': opp['action'],
                                'cex_price': round(cex_price, 2),
                                'dex_price': round(dex_price, 2),
                                'spread_percent': opp['spread'],
                                'net_profit_usd': opp['profit'],
                                'total_costs_usd': opp['costs']
                            })

                    # Відображення статистики
                    print(f"\n{'─' * 80}")
                    print(f"📊 СТАТИСТИКА ЗА ВЕСЬ ЧАС:")
                    print(f"   Перевірок:                 {self.stats['checks']}")
                    print(f"   Прибуткових CEX→DEX:       {self.stats['profitable_cex_to_dex']}")
                    print(f"   Прибуткових DEX→CEX:       {self.stats['profitable_dex_to_cex']}")
                    print(f"   Максимальний спред:        {self.stats['max_spread']:+.2f}%")
                    print(f"   Мінімальний спред:         {self.stats['min_spread']:+.2f}%")
                    print(f"   Максимальний прибуток:     ${self.stats['max_profit']:,.2f}")
                    print(f"   Знайдено арбітражів:       {self.opportunities_found}")

                    if TELEGRAM_ENABLED:
                        print(f"   Telegram сповіщень:        {self.telegram.last_notification_time > 0}")

                await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            logger.info("Моніторинг зупинено")
        except Exception as e:
            logger.error(f"Помилка в головному циклі: {e}")
            raise
        finally:
            bybit_task.cancel()
            await self.bybit.stop()
            await self.pancake.close()
            if self.telegram:
                await self.telegram.close()

    async def run(self):
        """Запуск монітора"""
        try:
            await self.monitor_loop()
        except KeyboardInterrupt:
            print("\n\n" + "=" * 80)
            print(f"🛑 МОНІТОРИНГ ЗУПИНЕНО")
            print(f"📊 ФІНАЛЬНА СТАТИСТИКА:")
            print(f"   Всього перевірок:           {self.stats['checks']}")
            print(f"   Прибуткових CEX→DEX:         {self.stats['profitable_cex_to_dex']}")
            print(f"   Прибуткових DEX→CEX:         {self.stats['profitable_dex_to_cex']}")
            print(f"   Максимальний спред:          {self.stats['max_spread']:+.2f}%")
            print(f"   Мінімальний спред:           {self.stats['min_spread']:+.2f}%")
            print(f"   Максимальний прибуток:       ${self.stats['max_profit']:,.2f}")
            print(f"   Знайдено арбітражів:         {self.opportunities_found}")
            print(f"📁 Лог збережено в:             {self.csv_file}")
            print("=" * 80)

            if self.telegram:
                await self.telegram.send_status_message(
                    f"🛑 <b>Моніторинг арбітражу зупинено</b>\n\n"
                    f"📊 Фінальна статистика:\n"
                    f"   • Перевірок: {self.stats['checks']}\n"
                    f"   • CEX→DEX: {self.stats['profitable_cex_to_dex']}\n"
                    f"   • DEX→CEX: {self.stats['profitable_dex_to_cex']}\n"
                    f"   • Макс. спред: {self.stats['max_spread']:+.2f}%\n"
                    f"   • Макс. прибуток: ${self.stats['max_profit']:,.2f}\n"
                    f"   • Знайдено арбітражів: {self.opportunities_found}\n\n"
                    f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                await self.telegram.close()
        except Exception as e:
            logger.error(f"Критична помилка: {e}")
            await self.bybit.stop()
            await self.pancake.close()
            if self.telegram:
                await self.telegram.close()


async def main():
    """Головна функція"""
    monitor = ArbitrageMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n✅ Програму зупинено користувачем")
    except Exception as e:
        logger.error(f"❌ Неочікувана помилка: {e}")