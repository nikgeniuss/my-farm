import sqlite3
import time
import json
import re
import requests
import threading
import random
import string
import hashlib
import secrets
import os
from functools import wraps
from collections import defaultdict
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, session, g, flash, jsonify
from flask_wtf.csrf import CSRFProtect

app = Flask(__name__)

app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', secrets.token_hex(32))
app.config['WTF_CSRF_TIME_LIMIT'] = 3600
app.permanent_session_lifetime = timedelta(days=30)

csrf = CSRFProtect()
csrf.init_app(app)

# ============= ДЛЯ RAILWAY =============
DB_PATH = os.environ.get('DATABASE_PATH', 'farm.db')

rate_limit_store = defaultdict(list)

def rate_limit(limit=30, window=60):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user_id = session.get('user_id', request.remote_addr)
            now = time.time()
            requests_list = rate_limit_store[user_id]
            requests_list = [t for t in requests_list if now - t < window]
            if len(requests_list) >= limit:
                flash(f'⚠️ Слишком много запросов. Подождите {window} секунд.', 'error')
                return redirect(request.referrer or url_for('index'))
            requests_list.append(now)
            rate_limit_store[user_id] = requests_list
            return f(*args, **kwargs)
        return decorated
    return decorator

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed):
    return hash_password(password) == hashed

USDT_TON_WALLET = os.environ.get('TON_WALLET', "UQBm50IIyJefw4l9hINwm6GkC8DzxBVTjB0OJMo1MoN9n71k")
TON_API = "https://toncenter.com/api/v2"

VEGETABLES = {
    'carrot': {'name': 'Морковь', 'cost': 10, 'income': 0.00000032, 'emoji': '🥕', 'color': '#FFA500'},
    'potato': {'name': 'Картофель', 'cost': 50, 'income': 0.00000172, 'emoji': '🥔', 'color': '#D2691E'},
    'onion': {'name': 'Лук', 'cost': 120, 'income': 0.00000435, 'emoji': '🧅', 'color': '#C41E3A'},
    'cabbage': {'name': 'Капуста', 'cost': 300, 'income': 0.0000114, 'emoji': '🥬', 'color': '#2E8B57'},
    'tomato': {'name': 'Томат', 'cost': 800, 'income': 0.0000321, 'emoji': '🍅', 'color': '#FF4500'},
    'cucumber': {'name': 'Огурец', 'cost': 1500, 'income': 0.0000638, 'emoji': '🥒', 'color': '#3CB371'},
    'pepper': {'name': 'Перец', 'cost': 3000, 'income': 0.000135, 'emoji': '🫑', 'color': '#32CD32'},
    'eggplant': {'name': 'Баклажан', 'cost': 7000, 'income': 0.000334, 'emoji': '🍆', 'color': '#800080'},
    'corn': {'name': 'Кукуруза', 'cost': 15000, 'income': 0.000762, 'emoji': '🌽', 'color': '#FFD700'},
    'watermelon': {'name': 'Арбуз', 'cost': 30000, 'income': 0.001286, 'emoji': '🍉', 'color': '#32CD32'}
}

UPGRADES = {
    'irrigation': {'name': 'Орошение', 'multiplier': 0.2, 'price_factor': 1.0, 'emoji': '💧', 'color': '#4A90E2'},
    'fertilizer': {'name': 'Удобрение', 'multiplier': 0.4, 'price_factor': 1.1, 'emoji': '💩', 'color': '#8B4513'},
    'light': {'name': 'Свет', 'multiplier': 0.7, 'price_factor': 1.2, 'emoji': '💡', 'color': '#FFD700'},
    'autocare': {'name': 'Авто-уход', 'multiplier': 1.1, 'price_factor': 1.3, 'emoji': '🤖', 'color': '#00CED1'},
    'genetics': {'name': 'Генетика', 'multiplier': 1.8, 'price_factor': 1.4, 'emoji': '🧬', 'color': '#FF1493'}
}

def get_storage_capacity(level):
    return 100 * (2 ** (level - 1))

def get_storage_upgrade_cost(level):
    return int(200 * (1.5 ** (level - 1)))

def get_upgrade_price(crop_key, upgrade_key):
    crop_income = VEGETABLES[crop_key]['income']
    upgrade_multiplier = UPGRADES[upgrade_key]['multiplier']
    price_factor = UPGRADES[upgrade_key]['price_factor']
    income_increase = crop_income * upgrade_multiplier
    seconds_in_year = 31536000
    price = (income_increase * seconds_in_year) / price_factor
    return int(price)

def generate_memo():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def init_db():
    with sqlite3.connect(DB_PATH, timeout=20) as conn:
        conn.execute('PRAGMA journal_mode=WAL')

        conn.execute('''CREATE TABLE IF NOT EXISTS users
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         login TEXT UNIQUE,
                         password TEXT,
                         balance REAL,
                         storage_level INTEGER DEFAULT 1,
                         grid_size INTEGER DEFAULT 3,
                         referrer_id INTEGER DEFAULT NULL,
                         created_at REAL DEFAULT NULL,
                         is_admin INTEGER DEFAULT 0)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS garden
                        (user_id INTEGER,
                         cell_id INTEGER,
                         crop TEXT,
                         upgrades_json TEXT,
                         last_harvest REAL,
                         PRIMARY KEY (user_id, cell_id))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS storage
                        (user_id INTEGER,
                         crop TEXT,
                         quantity REAL,
                         PRIMARY KEY (user_id, crop))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS referral_history
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         referrer_id INTEGER,
                         referred_id INTEGER,
                         bonus_amount REAL,
                         created_at REAL)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS deposit_requests
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         amount REAL,
                         memo TEXT UNIQUE,
                         txid TEXT,
                         status TEXT DEFAULT 'pending',
                         created_at REAL,
                         confirmed_at REAL)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS withdraw_requests
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         amount REAL,
                         wallet_address TEXT,
                         status TEXT DEFAULT 'pending',
                         created_at REAL,
                         processed_at REAL,
                         txid TEXT)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS user_2fa
                        (user_id INTEGER PRIMARY KEY,
                         telegram_id TEXT UNIQUE,
                         enabled INTEGER DEFAULT 0,
                         secret TEXT)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS login_codes
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         code TEXT,
                         expires_at REAL,
                         used INTEGER DEFAULT 0)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS ad_rewards
                        (user_id INTEGER PRIMARY KEY,
                         last_claim REAL,
                         daily_count INTEGER DEFAULT 0,
                         last_reset REAL,
                         ad_start_time REAL DEFAULT 0,
                         FOREIGN KEY (user_id) REFERENCES users(id))''')

        try:
            conn.execute('DROP TABLE IF EXISTS daily_bonus')
        except:
            pass

        conn.execute('''CREATE TABLE IF NOT EXISTS daily_bonus
                        (user_id INTEGER PRIMARY KEY,
                         last_claim_date TEXT,
                         streak INTEGER DEFAULT 0,
                         last_claim_time REAL,
                         FOREIGN KEY (user_id) REFERENCES users(id))''')

        try:
            conn.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0")
        except:
            pass

        try:
            conn.execute("ALTER TABLE ad_rewards ADD COLUMN ad_start_time REAL DEFAULT 0")
        except:
            pass

        try:
            conn.execute("ALTER TABLE users ADD COLUMN register_ip TEXT")
        except:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN register_ua TEXT")
        except:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN device_hash TEXT")
        except:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN bonus_balance REAL DEFAULT 0")
        except:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN farm_balance REAL DEFAULT 0")
        except:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN referrer_ip TEXT")
        except:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN referrer_ua TEXT")
        except:
            pass

        conn.execute('''CREATE TABLE IF NOT EXISTS activity_log
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_login TEXT,
                         event_type TEXT,
                         message TEXT,
                         created_at REAL)''')

        conn.execute('UPDATE users SET grid_size = 3 WHERE grid_size IS NULL')
        conn.execute('UPDATE users SET storage_level = 1 WHERE storage_level IS NULL')
        
        conn.execute("UPDATE users SET farm_balance = balance WHERE farm_balance = 0 AND balance > 0")
        conn.execute("UPDATE users SET bonus_balance = 0 WHERE bonus_balance IS NULL")

        admin_count = conn.execute('SELECT COUNT(*) as count FROM users WHERE is_admin = 1').fetchone()[0]
        if admin_count == 0:
            admin_login = 'admin'
            admin_password = hash_password('admin123')
            current_time = time.time()
            try:
                conn.execute('''
                    INSERT INTO users (login, password, balance, grid_size, storage_level, created_at, is_admin, farm_balance)
                    VALUES (?, ?, 100000, 3, 1, ?, 1, 100000)
                ''', (admin_login, admin_password, current_time))
                print(f"✅ Создан администратор: admin / admin123")
            except:
                pass

        conn.commit()

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH, timeout=20)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def log_activity(login, event_type, message):
    try:
        conn = get_db()
        conn.execute('INSERT INTO activity_log (user_login, event_type, message, created_at) VALUES (?, ?, ?, ?)',
                     (login, event_type, message, time.time()))
        conn.execute('DELETE FROM activity_log WHERE id NOT IN (SELECT id FROM activity_log ORDER BY id DESC LIMIT 200)')
        conn.commit()
    except Exception as e:
        print(f"log_activity error: {e}")

def calculate_income(crop_key, upgrades):
    base_income = VEGETABLES[crop_key]['income']
    multiplier = 1 + sum(upgrades.values())
    return base_income * multiplier

def harvest_crops(user_id, retries=3):
    for attempt in range(retries):
        try:
            conn = get_db()
            current_time = time.time()
            garden = conn.execute('SELECT cell_id, crop, upgrades_json, last_harvest FROM garden WHERE user_id = ?', (user_id,)).fetchall()
            harvested_crops = {}
            total_income_per_sec = 0

            for cell in garden:
                if cell['crop']:
                    upgrades = json.loads(cell['upgrades_json']) if cell['upgrades_json'] else {}
                    income_per_sec = calculate_income(cell['crop'], upgrades)
                    total_income_per_sec += income_per_sec
                    last_harvest = cell['last_harvest'] or current_time
                    harvest_time = current_time - last_harvest
                    harvest_amount = income_per_sec * harvest_time
                    if harvest_amount > 0:
                        crop_name = cell['crop']
                        harvested_crops[crop_name] = harvested_crops.get(crop_name, 0) + harvest_amount
                        conn.execute('UPDATE garden SET last_harvest = ? WHERE user_id = ? AND cell_id = ?',
                                    (current_time, user_id, cell['cell_id']))

            if harvested_crops:
                user_storage = conn.execute('SELECT storage_level FROM users WHERE id = ?', (user_id,)).fetchone()
                storage_capacity = get_storage_capacity(user_storage['storage_level'])
                current_storage = conn.execute('SELECT crop, quantity FROM storage WHERE user_id = ?', (user_id,)).fetchall()
                current_storage_dict = {row['crop']: row['quantity'] for row in current_storage}
                total_used = sum(current_storage_dict.values())
                total_to_add = sum(harvested_crops.values())

                if total_used + total_to_add > storage_capacity:
                    remaining_space = storage_capacity - total_used
                    if remaining_space > 0:
                        for crop, amount in harvested_crops.items():
                            if amount <= remaining_space:
                                current_storage_dict[crop] = current_storage_dict.get(crop, 0) + amount
                                remaining_space -= amount
                            else:
                                current_storage_dict[crop] = current_storage_dict.get(crop, 0) + remaining_space
                                break
                else:
                    for crop, amount in harvested_crops.items():
                        current_storage_dict[crop] = current_storage_dict.get(crop, 0) + amount

                for crop, quantity in current_storage_dict.items():
                    if quantity > 0:
                        conn.execute('INSERT OR REPLACE INTO storage (user_id, crop, quantity) VALUES (?, ?, ?)', (user_id, crop, quantity))
                    else:
                        conn.execute('DELETE FROM storage WHERE user_id = ? AND crop = ?', (user_id, crop))

            conn.commit()
            return total_income_per_sec
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < retries - 1:
                time.sleep(0.1)
                continue
            else:
                raise
    return 0

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('🔐 Пожалуйста, войдите в систему', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def get_user_with_stats(user_id, skip_harvest=False):
    try:
        if not skip_harvest:
            harvest_crops(user_id)
        conn = get_db()
        user = conn.execute('''
            SELECT 
                login, 
                balance, 
                grid_size, 
                storage_level, 
                bonus_balance, 
                farm_balance,
                register_ip,
                register_ua,
                device_hash,
                referrer_id,
                created_at,
                is_admin
            FROM users 
            WHERE id = ?
        ''', (user_id,)).fetchone()
        
        if not user:
            return None
            
        user_dict = dict(user)
        
        garden = conn.execute('SELECT crop, upgrades_json FROM garden WHERE user_id = ?', (user_id,)).fetchall()
        total_income_per_sec = 0
        for cell in garden:
            if cell['crop']:
                upgrades = json.loads(cell['upgrades_json']) if cell['upgrades_json'] else {}
                total_income_per_sec += calculate_income(cell['crop'], upgrades)
                
        user_dict['income_per_sec'] = total_income_per_sec
        user_dict['income_per_hour'] = total_income_per_sec * 3600
        user_dict['income_per_day'] = total_income_per_sec * 86400
        user_dict['income_per_month'] = total_income_per_sec * 2592000
        user_dict['storage_capacity'] = get_storage_capacity(user['storage_level'])
        
        storage_items = conn.execute('SELECT quantity FROM storage WHERE user_id = ?', (user_id,)).fetchall()
        user_dict['storage_used'] = sum(item['quantity'] for item in storage_items) if storage_items else 0
        
        return user_dict
    except Exception as e:
        print(f"Error in get_user_with_stats: {e}")
        return None

def get_referrals(user_id):
    conn = get_db()
    referrals = conn.execute('SELECT id, login, balance, created_at FROM users WHERE referrer_id = ? ORDER BY id DESC', (user_id,)).fetchall()
    referrals_list = []
    for ref in referrals:
        referrals_list.append({'id': ref['id'], 'login': ref['login'], 'balance': ref['balance'], 'created_at': ref['created_at'] or time.time()})
    history = conn.execute('SELECT referred_id, bonus_amount, created_at FROM referral_history WHERE referrer_id = ? ORDER BY created_at DESC', (user_id,)).fetchall()
    history_list = []
    for h in history:
        referred_user = conn.execute('SELECT login FROM users WHERE id = ?', (h['referred_id'],)).fetchone()
        history_list.append({'referred_id': h['referred_id'], 'referred_login': referred_user['login'] if referred_user else 'Неизвестно', 'bonus_amount': h['bonus_amount'], 'created_at': h['created_at']})
    return referrals_list, history_list

# ============= ЕЖЕДНЕВНЫЙ БОНУС =============

def get_daily_bonus_info(user_id):
    conn = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    today_date = datetime.now().date()

    try:
        bonus_data = conn.execute('SELECT last_claim_date, streak, last_claim_time FROM daily_bonus WHERE user_id = ?', (user_id,)).fetchone()

        can_claim = False
        bonus_streak = 0
        bonus_amount = 0

        if not bonus_data:
            can_claim = True
            bonus_streak = 0
            bonus_amount = 1
        else:
            last_claim_date = bonus_data['last_claim_date']
            streak = bonus_data['streak'] if bonus_data['streak'] is not None else 0

            if last_claim_date == today:
                can_claim = False
                bonus_streak = streak
                bonus_amount = min(streak + 1, 7) if streak < 7 else 7
            elif last_claim_date:
                try:
                    last_date = datetime.strptime(last_claim_date, '%Y-%m-%d').date()
                    if last_date == today_date - timedelta(days=1):
                        can_claim = True
                        bonus_streak = streak
                        bonus_amount = min(streak + 1, 7)
                    else:
                        can_claim = True
                        bonus_streak = 0
                        bonus_amount = 1
                except:
                    can_claim = True
                    bonus_streak = 0
                    bonus_amount = 1
            else:
                can_claim = True
                bonus_streak = 0
                bonus_amount = 1

        return {
            'can_claim': can_claim,
            'streak': bonus_streak,
            'amount': bonus_amount
        }
    except Exception as e:
        print(f"Error in get_daily_bonus_info: {e}")
        return {
            'can_claim': True,
            'streak': 0,
            'amount': 1
        }

def claim_daily_bonus_db(user_id):
    conn = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    today_date = datetime.now().date()

    try:
        bonus_data = conn.execute('SELECT last_claim_date, streak, last_claim_time FROM daily_bonus WHERE user_id = ?', (user_id,)).fetchone()

        if not bonus_data:
            bonus_amount = 1
            new_streak = 1
            conn.execute('INSERT INTO daily_bonus (user_id, last_claim_date, streak, last_claim_time) VALUES (?, ?, ?, ?)',
                         (user_id, today, 1, time.time()))
            conn.commit()
        else:
            last_claim_date = bonus_data['last_claim_date']
            streak = bonus_data['streak'] if bonus_data['streak'] is not None else 0

            if last_claim_date == today:
                return {'success': False, 'error': 'Сегодня бонус уже получен'}

            if last_claim_date:
                try:
                    last_date = datetime.strptime(last_claim_date, '%Y-%m-%d').date()
                    if last_date == today_date - timedelta(days=1):
                        new_streak = min(streak + 1, 7)
                        bonus_amount = new_streak
                    else:
                        new_streak = 1
                        bonus_amount = 1
                except:
                    new_streak = 1
                    bonus_amount = 1
            else:
                new_streak = 1
                bonus_amount = 1

            conn.execute('UPDATE daily_bonus SET last_claim_date = ?, streak = ?, last_claim_time = ? WHERE user_id = ?',
                         (today, new_streak, time.time(), user_id))
            conn.commit()

        conn.execute('UPDATE users SET bonus_balance = bonus_balance + ? WHERE id = ?', (bonus_amount, user_id))
        conn.commit()

        return {'success': True, 'bonus': bonus_amount, 'streak': new_streak}

    except Exception as e:
        print(f"Error in claim_daily_bonus_db: {e}")
        try:
            conn.rollback()
        except:
            pass
        return {'success': False, 'error': str(e)}

# ============= ТАБЛИЦА ЛИДЕРОВ =============

def get_leaderboard_data():
    conn = get_db()

    top_deposits = conn.execute('''
        SELECT u.login, SUM(d.amount) as total
        FROM deposit_requests d
        JOIN users u ON d.user_id = u.id
        WHERE d.status = 'confirmed'
        GROUP BY u.id
        ORDER BY total DESC
        LIMIT 100
    ''').fetchall()

    top_withdraws = conn.execute('''
        SELECT u.login, SUM(w.amount) as total
        FROM withdraw_requests w
        JOIN users u ON w.user_id = u.id
        WHERE w.status = 'completed'
        GROUP BY u.id
        ORDER BY total DESC
        LIMIT 100
    ''').fetchall()

    top_income = []
    users = conn.execute('SELECT id, login FROM users').fetchall()
    for user in users:
        garden = conn.execute('SELECT crop, upgrades_json FROM garden WHERE user_id = ?', (user['id'],)).fetchall()
        total_income = 0
        for cell in garden:
            if cell['crop']:
                upgrades = json.loads(cell['upgrades_json']) if cell['upgrades_json'] else {}
                total_income += calculate_income(cell['crop'], upgrades)
        if total_income > 0:
            top_income.append({'login': user['login'], 'income_per_hour': total_income * 3600})
    top_income.sort(key=lambda x: x['income_per_hour'], reverse=True)
    top_income = top_income[:100]

    top_referrals = conn.execute('''
        SELECT u.login, COUNT(r.referred_id) as total
        FROM users u
        LEFT JOIN referral_history r ON u.id = r.referrer_id
        GROUP BY u.id
        ORDER BY total DESC
        LIMIT 100
    ''').fetchall()

    return {
        'deposits': top_deposits,
        'withdraws': top_withdraws,
        'income': top_income,
        'referrals': top_referrals
    }

# ============= ПРОВЕРКА ТРАНЗАКЦИЙ TON =============

def check_ton_transactions():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        pending = conn.execute('SELECT id, user_id, amount, memo FROM deposit_requests WHERE status = "pending"').fetchall()
        if not pending:
            conn.close()
            return
        for pending_req in pending:
            url = f"{TON_API}/getTransactions"
            params = {'address': USDT_TON_WALLET, 'limit': 50}
            try:
                response = requests.get(url, params=params, timeout=15)
                if response.status_code != 200:
                    continue
                data = response.json()
                if not data.get('ok'):
                    continue
                for tx in data.get('result', []):
                    txid = tx.get('transaction_id', {}).get('hash')
                    if not txid:
                        continue
                    existing = conn.execute('SELECT id FROM deposit_requests WHERE txid = ?', (txid,)).fetchone()
                    if existing:
                        continue
                    in_msg = tx.get('in_msg', {})
                    if in_msg.get('source') == USDT_TON_WALLET:
                        continue
                    comment = in_msg.get('message', '')
                    if comment and not comment.isprintable():
                        try:
                            comment = bytes.fromhex(comment).decode('utf-8', errors='ignore')
                        except:
                            comment = ''
                    comment = comment.strip()
                    if comment != pending_req['memo']:
                        continue
                    value_nano = int(in_msg.get('value', '0'))
                    value_ton = value_nano / 1000000000
                    if abs(pending_req['amount'] - value_ton) > 0.01:
                        continue
                    coins_amount = int(value_ton * 100)
                    conn.execute('UPDATE users SET farm_balance = farm_balance + ? WHERE id = ?', (coins_amount, pending_req['user_id']))
                    conn.execute('UPDATE deposit_requests SET status = "confirmed", txid = ?, confirmed_at = ? WHERE id = ?', (txid, time.time(), pending_req['id']))
                    conn.commit()
                    print(f"✅ Зачислено {value_ton} USDT пользователю {pending_req['user_id']}")
            except Exception as e:
                print(f"Ошибка: {e}")
                continue
        conn.close()
    except Exception as e:
        print(f"Ошибка: {e}")

def start_transaction_monitor():
    def monitor():
        while True:
            try:
                check_ton_transactions()
            except Exception as e:
                print(f"Monitor error: {e}")
            time.sleep(15)
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    print("🟢 Мониторинг запущен")

# ============= ФИЛЬТРЫ =============

@app.template_filter('timestamp_to_date')
def timestamp_to_date(timestamp):
    if timestamp:
        return datetime.fromtimestamp(timestamp).strftime('%d.%m.%Y %H:%M')
    return '—'

@app.template_filter('timestamp_to_datetime')
def timestamp_to_datetime(timestamp):
    if timestamp:
        return datetime.fromtimestamp(timestamp).strftime('%d.%m.%Y %H:%M:%S')
    return '—'

# ============= ОСНОВНЫЕ МАРШРУТЫ =============

@app.route('/')
@login_required
@rate_limit(limit=60, window=60)
def index():
    user = get_user_with_stats(session['user_id'], skip_harvest=False)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))

    bonus_info = get_daily_bonus_info(session['user_id'])

    conn = get_db()
    garden = conn.execute('SELECT cell_id, crop, upgrades_json FROM garden WHERE user_id = ?', (session['user_id'],)).fetchall()
    garden_dict = {row['cell_id']: {'crop': row['crop'], 'upgrades': json.loads(row['upgrades_json']) if row['upgrades_json'] else {}} for row in garden}
    grid_size = user['grid_size'] if user['grid_size'] else 3
    total_cells = grid_size * grid_size
    full_garden = {}
    for i in range(total_cells):
        if i in garden_dict:
            full_garden[i] = garden_dict[i]
        else:
            full_garden[i] = {'crop': None, 'upgrades': {}}

    return render_template('index.html',
                          user=user,
                          garden=full_garden,
                          vegetables=VEGETABLES,
                          upgrades=UPGRADES,
                          grid_size=grid_size,
                          total_cells=total_cells,
                          income_per_sec=user['income_per_sec'],
                          income_per_hour=user['income_per_hour'],
                          income_per_day=user['income_per_day'],
                          income_per_month=user['income_per_month'],
                          can_claim_bonus=bonus_info['can_claim'],
                          bonus_streak=bonus_info['streak'],
                          bonus_amount=bonus_info['amount'])

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        action = request.form.get('action')
        login = request.form['login']
        password = request.form['password']
        if not login or not password:
            flash('❌ Логин и пароль не могут быть пустыми!', 'error')
            return redirect(url_for('login'))
        if any('а' <= char <= 'я' or 'А' <= char <= 'Я' for char in login):
            flash('❌ Логин должен содержать только латинские буквы', 'error')
            return redirect(url_for('login'))
        if len(login) < 3 or len(login) > 20:
            flash('❌ Логин должен быть от 3 до 20 символов!', 'error')
            return redirect(url_for('login'))
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', login):
            flash('❌ Недопустимые символы в логине', 'error')
            return redirect(url_for('login'))
        conn = get_db()
        if action == 'login':
            user = conn.execute('SELECT id, password, is_admin FROM users WHERE login = ?', (login,)).fetchone()
            if not user:
                flash('❌ Пользователь не найден', 'error')
                return redirect(url_for('login'))
            if not verify_password(password, user['password']):
                flash('❌ Неверный пароль', 'error')
                return redirect(url_for('login'))
            user_2fa = conn.execute('SELECT enabled FROM user_2fa WHERE user_id = ?', (user['id'],)).fetchone()
            if user_2fa and user_2fa['enabled']:
                session['pending_user_id'] = user['id']
                session['pending_is_admin'] = bool(user['is_admin'])
                create_login_code(user['id'])
                flash('📱 Код подтверждения отправлен в Telegram', 'info')
                return redirect(url_for('verify_2fa'))
            session['user_id'] = user['id']
            session['is_admin'] = bool(user['is_admin'])
            flash('✅ Добро пожаловать!', 'success')
            return redirect(url_for('index'))
        elif action == 'register':
            existing = conn.execute('SELECT id FROM users WHERE login = ?', (login,)).fetchone()
            if existing:
                flash('❌ Логин уже занят', 'error')
                return redirect(url_for('login'))
            if len(password) < 4 or len(password) > 30:
                flash('❌ Пароль должен быть от 4 до 30 символов', 'error')
                return redirect(url_for('login'))
            if any('а' <= char <= 'я' or 'А' <= char <= 'Я' for char in password):
                flash('❌ Пароль должен содержать только латинские буквы', 'error')
                return redirect(url_for('login'))
            
            register_ip = request.remote_addr
            register_ua = request.headers.get('User-Agent', '')
            device_hash = request.form.get('device_hash', '')
            
            ip_count = conn.execute(
                'SELECT COUNT(*) FROM users WHERE register_ip = ? AND created_at > ?',
                (register_ip, time.time() - 86400)
            ).fetchone()[0]
            if ip_count >= 3:
                flash('❌ С одного IP не более 3 аккаунтов за 24 часа', 'error')
                return redirect(url_for('login'))
            
            if device_hash:
                existing_device = conn.execute('SELECT id FROM users WHERE device_hash = ?', (device_hash,)).fetchone()
                if existing_device:
                    flash('❌ С этого браузера уже регистрировались', 'error')
                    return redirect(url_for('login'))
            
            referrer_id = request.form.get('referrer_id')
            referrer_ip = None
            referrer_ua = None
            
            if referrer_id and not referrer_id.isdigit():
                referrer_user = conn.execute('SELECT id, register_ip, register_ua FROM users WHERE login = ?', (referrer_id,)).fetchone()
                if referrer_user:
                    referrer_id = referrer_user['id']
                    referrer_ip = referrer_user['register_ip']
                    referrer_ua = referrer_user['register_ua']
                else:
                    referrer_id = None
            elif referrer_id and referrer_id.isdigit():
                referrer_user = conn.execute('SELECT id, register_ip, register_ua FROM users WHERE id = ?', (int(referrer_id),)).fetchone()
                if referrer_user:
                    referrer_id = int(referrer_id)
                    referrer_ip = referrer_user['register_ip']
                    referrer_ua = referrer_user['register_ua']
                else:
                    referrer_id = None
            else:
                referrer_id = None
            
            if referrer_id and referrer_ip == register_ip:
                flash('❌ Нельзя приглашать себя с одного IP', 'error')
                return redirect(url_for('login'))
            
            def check_cycle(current_id, target_id, depth=0):
                if depth > 3:
                    return False
                parent = conn.execute('SELECT referrer_id FROM users WHERE id = ?', (current_id,)).fetchone()
                if parent and parent['referrer_id'] == target_id:
                    return True
                return parent and parent['referrer_id'] and check_cycle(parent['referrer_id'], target_id, depth+1)
            
            if referrer_id and check_cycle(referrer_id, None):
                flash('❌ Обнаружен цикл в реферальной сети', 'error')
                return redirect(url_for('login'))
            
            current_time = time.time()
            hashed_password = hash_password(password)
            
            cursor = conn.execute('''INSERT INTO users 
                (login, password, balance, grid_size, storage_level, referrer_id, created_at, is_admin,
                 register_ip, register_ua, device_hash, bonus_balance, farm_balance, referrer_ip, referrer_ua) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (login, hashed_password, 100, 3, 1, referrer_id, current_time, 0,
                 register_ip, register_ua, device_hash, 100, 0, referrer_ip, referrer_ua))
            user_id = cursor.lastrowid
            
            for cell_id in range(9):
                conn.execute('INSERT INTO garden (user_id, cell_id, crop, upgrades_json, last_harvest) VALUES (?, ?, ?, ?, ?)',
                            (user_id, cell_id, None, '{}', current_time))
            
            if referrer_id:
                conn.execute('UPDATE users SET bonus_balance = bonus_balance + 100 WHERE id = ?', (user_id,))
                conn.execute('UPDATE users SET bonus_balance = bonus_balance + 100 WHERE id = ?', (referrer_id,))
                conn.execute('INSERT INTO referral_history (referrer_id, referred_id, bonus_amount, created_at) VALUES (?, ?, ?, ?)',
                            (referrer_id, user_id, 100, current_time))
                log_activity(login, 'referral', f'🎉 {login} зарегистрировался по реферальной ссылке')
                flash('🎉 Аккаунт создан! Получено 200 Coin (бонусные)!', 'success')
            else:
                log_activity(login, 'register', f'🆕 {login} присоединился к игре')
                flash('🎉 Аккаунт создан! В подарок 100 Coin (бонусные)!', 'success')
            
            conn.commit()
            session['user_id'] = user_id
            session['is_admin'] = 0
            return redirect(url_for('index'))
    referrer_id = request.args.get('ref')
    return render_template('login.html', referrer_id=referrer_id)

@app.route('/logout')
def logout():
    session.clear()
    flash('👋 До свидания!', 'info')
    return redirect(url_for('login'))

@app.route('/plant/<int:cell_id>', methods=['POST'])
@login_required
@rate_limit(limit=20, window=60)
def plant(cell_id):
    crop_key = request.form.get('crop')
    if not crop_key or crop_key not in VEGETABLES:
        flash('❌ Ошибка выбора овоща', 'error')
        return redirect(url_for('index'))

    harvest_crops(session['user_id'])
    conn = get_db()
    user = conn.execute('SELECT bonus_balance, farm_balance FROM users WHERE id = ?', (session['user_id'],)).fetchone()
    crop_cost = VEGETABLES[crop_key]['cost']
    crop_name = VEGETABLES[crop_key]['name']

    total_balance = user['bonus_balance'] + user['farm_balance']
    
    if total_balance >= crop_cost:
        if user['bonus_balance'] >= crop_cost:
            new_bonus = user['bonus_balance'] - crop_cost
            conn.execute('UPDATE users SET bonus_balance = ? WHERE id = ?', (new_bonus, session['user_id']))
        else:
            remaining = crop_cost - user['bonus_balance']
            conn.execute('UPDATE users SET bonus_balance = 0, farm_balance = farm_balance - ? WHERE id = ?', (remaining, session['user_id']))

        conn.execute('''INSERT OR REPLACE INTO garden (user_id, cell_id, crop, upgrades_json, last_harvest)
                        VALUES (?, ?, ?, ?, ?)''',
                    (session['user_id'], cell_id, crop_key, '{}', time.time()))
        conn.commit()

        user_login = conn.execute('SELECT login FROM users WHERE id = ?', (session['user_id'],)).fetchone()['login']
        log_activity(user_login, 'plant', f'🌱 {user_login} посадил {crop_name}')
        flash(f'✅ {crop_name} посажен! -{crop_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {crop_cost} Coin', 'error')

    return redirect(url_for('index'))

@app.route('/upgrade/<int:cell_id>', methods=['POST'])
@login_required
@rate_limit(limit=20, window=60)
def upgrade(cell_id):
    upgrade_key = request.form.get('upgrade')
    if not upgrade_key or upgrade_key not in UPGRADES:
        flash('❌ Ошибка выбора апгрейда', 'error')
        return redirect(url_for('index'))
    harvest_crops(session['user_id'])
    conn = get_db()
    cell = conn.execute('SELECT crop, upgrades_json FROM garden WHERE user_id = ? AND cell_id = ?',
                       (session['user_id'], cell_id)).fetchone()
    if not cell or not cell['crop']:
        flash('❌ На участке ничего не посажено', 'error')
        return redirect(url_for('index'))
    upgrades = json.loads(cell['upgrades_json']) if cell['upgrades_json'] else {}
    if upgrade_key in upgrades:
        flash(f'⚠️ Апгрейд уже куплен', 'warning')
        return redirect(url_for('index'))
    upgrade_cost = get_upgrade_price(cell['crop'], upgrade_key)
    upgrade_name = UPGRADES[upgrade_key]['name']
    upgrade_multiplier = UPGRADES[upgrade_key]['multiplier']
    user = conn.execute('SELECT bonus_balance, farm_balance FROM users WHERE id = ?', (session['user_id'],)).fetchone()
    upgrade_user_login = conn.execute('SELECT login FROM users WHERE id = ?', (session['user_id'],)).fetchone()['login']
    
    total_balance = user['bonus_balance'] + user['farm_balance']
    
    if total_balance >= upgrade_cost:
        if user['bonus_balance'] >= upgrade_cost:
            new_bonus = user['bonus_balance'] - upgrade_cost
            conn.execute('UPDATE users SET bonus_balance = ? WHERE id = ?', (new_bonus, session['user_id']))
        else:
            remaining = upgrade_cost - user['bonus_balance']
            conn.execute('UPDATE users SET bonus_balance = 0, farm_balance = farm_balance - ? WHERE id = ?', (remaining, session['user_id']))
        
        upgrades[upgrade_key] = upgrade_multiplier
        conn.execute('UPDATE garden SET upgrades_json = ? WHERE user_id = ? AND cell_id = ?',
                    (json.dumps(upgrades), session['user_id'], cell_id))
        conn.commit()
        log_activity(upgrade_user_login, 'upgrade', f'⚡ {upgrade_user_login} купил апгрейд {upgrade_name}')
        flash(f'✨ {upgrade_name} куплен! +{upgrade_multiplier*100:.0f}%! -{upgrade_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {upgrade_cost} Coin', 'error')
    return redirect(url_for('index'))

@app.route('/expand_garden', methods=['POST'])
@login_required
@rate_limit(limit=5, window=300)
def expand_garden():
    harvest_crops(session['user_id'])
    conn = get_db()
    user = conn.execute('SELECT bonus_balance, farm_balance, grid_size FROM users WHERE id = ?', (session['user_id'],)).fetchone()
    current_size = user['grid_size'] if user['grid_size'] else 3
    expand_costs = {3: 2100, 4: 4500, 5: 9900}
    if current_size >= 6:
        flash('🎉 Максимальный размер огорода!', 'warning')
        return redirect(url_for('index'))
    new_size = current_size + 1
    expand_cost = expand_costs[current_size]
    expand_user_login = conn.execute('SELECT login FROM users WHERE id = ?', (session['user_id'],)).fetchone()['login']
    
    total_balance = user['bonus_balance'] + user['farm_balance']
    
    if total_balance >= expand_cost:
        if user['bonus_balance'] >= expand_cost:
            new_bonus = user['bonus_balance'] - expand_cost
            conn.execute('UPDATE users SET bonus_balance = ? WHERE id = ?', (new_bonus, session['user_id']))
        else:
            remaining = expand_cost - user['bonus_balance']
            conn.execute('UPDATE users SET bonus_balance = 0, farm_balance = farm_balance - ? WHERE id = ?', (remaining, session['user_id']))
        
        conn.execute('UPDATE users SET grid_size = ? WHERE id = ?', (new_size, session['user_id']))
        old_cells = current_size * current_size
        new_cells = new_size * new_size
        current_time = time.time()
        for cell_id in range(old_cells, new_cells):
            conn.execute('INSERT OR IGNORE INTO garden (user_id, cell_id, crop, upgrades_json, last_harvest) VALUES (?, ?, ?, ?, ?)',
                        (session['user_id'], cell_id, None, '{}', current_time))
        conn.commit()
        log_activity(expand_user_login, 'expand', f'🌾 {expand_user_login} расширил огород до {new_size}x{new_size}')
        flash(f'🌾 Огород расширен до {new_size}x{new_size}! -{expand_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {expand_cost} Coin', 'error')
    return redirect(url_for('index'))

@app.route('/storage')
@login_required
@rate_limit(limit=30, window=60)
def storage():
    user = get_user_with_stats(session['user_id'], skip_harvest=False)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    conn = get_db()
    storage_items = conn.execute('SELECT crop, quantity FROM storage WHERE user_id = ?', (session['user_id'],)).fetchall()
    storage_dict = {row['crop']: row['quantity'] for row in storage_items}
    return render_template('storage.html', user=user, storage_dict=storage_dict, vegetables=VEGETABLES, get_storage_upgrade_cost=get_storage_upgrade_cost, get_storage_capacity=get_storage_capacity, income_per_sec=user['income_per_sec'], income_per_hour=user['income_per_hour'], income_per_day=user['income_per_day'], income_per_month=user['income_per_month'])

@app.route('/sell/<crop>', methods=['POST'])
@login_required
@rate_limit(limit=20, window=60)
def sell_crop(crop):
    if crop not in VEGETABLES:
        flash('❌ Неизвестная культура', 'error')
        return redirect(url_for('storage'))
    
    quantity = float(request.form.get('quantity', 0))
    if quantity <= 0:
        flash('❌ Укажите корректное количество', 'error')
        return redirect(url_for('storage'))
    
    conn = get_db()
    storage_item = conn.execute('SELECT quantity FROM storage WHERE user_id = ? AND crop = ?', (session['user_id'], crop)).fetchone()
    
    if not storage_item or storage_item['quantity'] < quantity - 0.000001:
        flash(f'❌ Недостаточно {VEGETABLES[crop]["name"]}', 'error')
        return redirect(url_for('storage'))
    
    total_earned = quantity
    new_quantity = storage_item['quantity'] - quantity
    
    if new_quantity <= 0.000001:
        conn.execute('DELETE FROM storage WHERE user_id = ? AND crop = ?', (session['user_id'], crop))
    else:
        conn.execute('UPDATE storage SET quantity = ? WHERE user_id = ? AND crop = ?', (new_quantity, session['user_id'], crop))
    
    conn.execute('UPDATE users SET farm_balance = farm_balance + ? WHERE id = ?', (total_earned, session['user_id']))
    conn.commit()
    
    flash(f'💰 Продано {quantity:.4f} {VEGETABLES[crop]["name"]} за {total_earned:.4f} Coin (зачислено на фермерский баланс)', 'success')
    return redirect(url_for('storage'))

@app.route('/sell_all/<crop>', methods=['POST'])
@login_required
@rate_limit(limit=20, window=60)
def sell_all_crop(crop):
    if crop not in VEGETABLES:
        flash('❌ Неизвестная культура', 'error')
        return redirect(url_for('storage'))
    
    conn = get_db()
    storage_item = conn.execute('SELECT quantity FROM storage WHERE user_id = ? AND crop = ?', (session['user_id'], crop)).fetchone()
    
    if not storage_item or storage_item['quantity'] <= 0:
        flash(f'❌ {VEGETABLES[crop]["name"]} нет на складе', 'error')
        return redirect(url_for('storage'))
    
    quantity = storage_item['quantity']
    total_earned = quantity
    
    conn.execute('DELETE FROM storage WHERE user_id = ? AND crop = ?', (session['user_id'], crop))
    conn.execute('UPDATE users SET farm_balance = farm_balance + ? WHERE id = ?', (total_earned, session['user_id']))
    conn.commit()
    
    flash(f'💰 Продано всё {VEGETABLES[crop]["name"]} ({quantity:.4f} шт) за {total_earned:.4f} Coin (зачислено на фермерский баланс)', 'success')
    return redirect(url_for('storage'))

@app.route('/sell_all_storage', methods=['POST'])
@login_required
@rate_limit(limit=10, window=60)
def sell_all_storage():
    conn = get_db()
    storage_items = conn.execute('SELECT crop, quantity FROM storage WHERE user_id = ?', (session['user_id'],)).fetchall()
    if not storage_items:
        flash('❌ Склад пуст', 'error')
        return redirect(url_for('storage'))
    
    total_earned = sum(item['quantity'] for item in storage_items)
    total_items = sum(item['quantity'] for item in storage_items)
    
    conn.execute('UPDATE users SET farm_balance = farm_balance + ? WHERE id = ?', (total_earned, session['user_id']))
    conn.execute('DELETE FROM storage WHERE user_id = ?', (session['user_id'],))
    conn.commit()
    
    flash(f'💰 Продано всё ({total_items:.4f} шт) за {total_earned:.4f} Coin (зачислено на фермерский баланс)', 'success')
    return redirect(url_for('storage'))

@app.route('/upgrade_storage', methods=['POST'])
@login_required
@rate_limit(limit=10, window=300)
def upgrade_storage():
    harvest_crops(session['user_id'])
    conn = get_db()
    user = conn.execute('SELECT bonus_balance, farm_balance, storage_level FROM users WHERE id = ?', (session['user_id'],)).fetchone()
    current_level = user['storage_level']
    upgrade_cost = get_storage_upgrade_cost(current_level)
    new_capacity = get_storage_capacity(current_level + 1)
    
    total_balance = user['bonus_balance'] + user['farm_balance']
    
    if total_balance >= upgrade_cost:
        if user['bonus_balance'] >= upgrade_cost:
            new_bonus = user['bonus_balance'] - upgrade_cost
            conn.execute('UPDATE users SET bonus_balance = ? WHERE id = ?', (new_bonus, session['user_id']))
        else:
            remaining = upgrade_cost - user['bonus_balance']
            conn.execute('UPDATE users SET bonus_balance = 0, farm_balance = farm_balance - ? WHERE id = ?', (remaining, session['user_id']))
        
        conn.execute('UPDATE users SET storage_level = ? WHERE id = ?', (current_level + 1, session['user_id']))
        conn.commit()
        flash(f'🏚️ Склад улучшен до {current_level + 1} уровня! Вместимость: {new_capacity:.0f} шт. -{upgrade_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {upgrade_cost} Coin', 'error')
    return redirect(url_for('storage'))

@app.route('/referrals')
@login_required
@rate_limit(limit=30, window=60)
def referrals():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    referral_link = f"{request.scheme}://{request.host}/login?ref={user['login']}"
    referrals_list, history_list = get_referrals(session['user_id'])
    total_bonus = sum(h['bonus_amount'] for h in history_list)
    return render_template('referrals.html', user=user, referral_link=referral_link, referrals=referrals_list, history=history_list, total_bonus=total_bonus, income_per_sec=user['income_per_sec'], income_per_hour=user['income_per_hour'], income_per_day=user['income_per_day'], income_per_month=user['income_per_month'])

@app.route('/deposit')
@login_required
@rate_limit(limit=30, window=60)
def deposit():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    conn = get_db()
    deposit_requests = conn.execute('SELECT id, amount, memo, txid, status, created_at FROM deposit_requests WHERE user_id = ? ORDER BY created_at DESC', (session['user_id'],)).fetchall()
    return render_template('deposit.html', user=user, requests=deposit_requests, wallet_address=USDT_TON_WALLET, income_per_sec=user['income_per_sec'], income_per_hour=user['income_per_hour'], income_per_day=user['income_per_day'], income_per_month=user['income_per_month'])

@app.route('/create_deposit', methods=['POST'])
@login_required
@rate_limit(limit=5, window=300)
def create_deposit():
    amount = float(request.form.get('amount', 0))
    if amount < 1:
        flash('❌ Минимальная сумма 1 USDT', 'error')
        return redirect(url_for('deposit'))
    if amount > 10000:
        flash('❌ Максимальная сумма 10000 USDT', 'error')
        return redirect(url_for('deposit'))
    current_time = time.time()
    memo = generate_memo()
    conn = get_db()
    existing = conn.execute('SELECT id FROM deposit_requests WHERE memo = ?', (memo,)).fetchone()
    while existing:
        memo = generate_memo()
        existing = conn.execute('SELECT id FROM deposit_requests WHERE memo = ?', (memo,)).fetchone()
    conn.execute('INSERT INTO deposit_requests (user_id, amount, memo, created_at, status) VALUES (?, ?, ?, ?, "pending")',
                 (session['user_id'], amount, memo, current_time))
    conn.commit()
    dep_login = conn.execute('SELECT login FROM users WHERE id = ?', (session['user_id'],)).fetchone()['login']
    log_activity(dep_login, 'deposit', f'💎 {dep_login} пополнил баланс на {amount} USDT')
    flash(f'✅ Заявка на {amount} USDT создана! Мемо: {memo}', 'success')
    return redirect(url_for('deposit'))

@app.route('/withdraw')
@login_required
@rate_limit(limit=30, window=60)
def withdraw():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))

    conn = get_db()
    withdraw_requests = conn.execute('SELECT id, amount, wallet_address, status, created_at FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC',
                                      (session['user_id'],)).fetchall()
    conn.close()

    return render_template('withdraw.html', user=user, withdraw_requests=withdraw_requests,
                          income_per_sec=user['income_per_sec'],
                          income_per_hour=user['income_per_hour'],
                          income_per_day=user['income_per_day'],
                          income_per_month=user['income_per_month'])

@app.route('/create_withdraw', methods=['POST'])
@login_required
@rate_limit(limit=3, window=300)
def create_withdraw():
    amount = float(request.form.get('amount', 0))
    wallet = request.form.get('wallet', '').strip()
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    
    if amount < 10:
        flash('❌ Минимальная сумма 10 Coin', 'error')
        return redirect(url_for('withdraw'))
    
    if amount > user['farm_balance']:
        flash('❌ Вывести можно только заработанные на продаже овощей Coin (фермерский баланс). Бонусные Coin тратятся только на покупки.', 'error')
        return redirect(url_for('withdraw'))
    
    if not wallet or not (wallet.startswith('EQ') or wallet.startswith('UQ')) or len(wallet) < 40:
        flash('❌ Неверный формат TON адреса', 'error')
        return redirect(url_for('withdraw'))
    
    conn = get_db()
    conn.execute('UPDATE users SET farm_balance = farm_balance - ? WHERE id = ?', (amount, session['user_id']))
    conn.execute('INSERT INTO withdraw_requests (user_id, amount, wallet_address, created_at, status) VALUES (?, ?, ?, ?, "pending")',
                 (session['user_id'], amount, wallet, time.time()))
    conn.commit()
    wd_login = conn.execute('SELECT login FROM users WHERE id = ?', (session['user_id'],)).fetchone()['login']
    log_activity(wd_login, 'withdraw', f'💸 {wd_login} вывел {int(amount)} Coin')
    flash(f'✅ Заявка на вывод {amount} Coin создана! Администратор обработает её в ближайшее время.', 'success')
    return redirect(url_for('withdraw'))

@app.route('/about')
@login_required
@rate_limit(limit=30, window=60)
def about():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    return render_template('about.html', user=user, income_per_sec=user['income_per_sec'], income_per_hour=user['income_per_hour'], income_per_day=user['income_per_day'], income_per_month=user['income_per_month'])

@app.route('/leaderboard')
@login_required
@rate_limit(limit=30, window=60)
def leaderboard():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))

    data = get_leaderboard_data()
    current_user_login = user['login']

    return render_template('leaderboard.html',
                          user=user,
                          current_user=current_user_login,
                          top_deposits=data['deposits'],
                          top_withdraws=data['withdraws'],
                          top_income=data['income'],
                          top_referrals=data['referrals'],
                          income_per_sec=user['income_per_sec'],
                          income_per_hour=user['income_per_hour'],
                          income_per_day=user['income_per_day'],
                          income_per_month=user['income_per_month'])

@app.route('/claim_daily_bonus', methods=['POST'])
@login_required
def claim_daily_bonus():
    try:
        result = claim_daily_bonus_db(session['user_id'])
        if result['success']:
            flash(f'🎁 Ежедневный бонус +{result["bonus"]} Coin (бонусные, не выводятся)! Серия: {result["streak"]} дней', 'success')
            return jsonify({'success': True, 'bonus': result['bonus'], 'streak': result['streak']})
        else:
            return jsonify({'success': False, 'error': result.get('error', 'Ошибка')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/stats')
@login_required
@rate_limit(limit=60, window=60)
def api_stats():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        return {'error': 'User not found'}, 404
    return {
        'farm_balance': user.get('farm_balance', 0),
        'bonus_balance': user.get('bonus_balance', 0),
        'income_per_sec': user['income_per_sec'], 
        'income_per_hour': user['income_per_hour'], 
        'income_per_day': user['income_per_day'], 
        'income_per_month': user['income_per_month'], 
        'grid_size': user['grid_size'], 
        'storage_level': user['storage_level'], 
        'storage_capacity': user['storage_capacity'], 
        'storage_used': user['storage_used']
    }

@app.route('/api/activity')
@login_required
def api_activity():
    conn = get_db()
    events = conn.execute(
        'SELECT user_login, event_type, message, created_at FROM activity_log ORDER BY id DESC LIMIT 30'
    ).fetchall()
    return jsonify([dict(e) for e in events])

# ============= АДМИН-ПАНЕЛЬ =============

ADMIN_SECRET = secrets.token_hex(16)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', "")

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('🔐 Пожалуйста, войдите в систему', 'warning')
            return redirect(url_for('login'))
        conn = get_db()
        user = conn.execute('SELECT is_admin FROM users WHERE id = ?', (session['user_id'],)).fetchone()
        if not user or not user['is_admin']:
            flash('⛔ Доступ запрещен', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def send_telegram_code(user_id, code):
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        import asyncio
        from telegram import Bot
        
        async def send():
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            conn = get_db()
            user_2fa = conn.execute('SELECT telegram_id FROM user_2fa WHERE user_id = ? AND enabled = 1', (user_id,)).fetchone()
            if not user_2fa:
                return False
            await bot.send_message(chat_id=user_2fa['telegram_id'], text=f"🔐 Ваш код: {code}\nДействителен 5 минут")
            return True
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(send())
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

def generate_2fa_code():
    return ''.join(str(random.randint(0, 9)) for _ in range(6))

def create_login_code(user_id):
    code = generate_2fa_code()
    expires_at = time.time() + 300
    conn = get_db()
    conn.execute('INSERT INTO login_codes (user_id, code, expires_at, used) VALUES (?, ?, ?, 0)', (user_id, code, expires_at))
    conn.commit()
    send_telegram_code(user_id, code)
    return code

def verify_login_code(user_id, code):
    conn = get_db()
    login_code = conn.execute('SELECT id, expires_at FROM login_codes WHERE user_id = ? AND code = ? AND used = 0 ORDER BY id DESC LIMIT 1', (user_id, code)).fetchone()
    if not login_code:
        return False
    if time.time() > login_code['expires_at']:
        return False
    conn.execute('UPDATE login_codes SET used = 1 WHERE id = ?', (login_code['id'],))
    conn.commit()
    return True

@app.route('/setup_2fa', methods=['GET', 'POST'])
@login_required
def setup_2fa():
    conn = get_db()
    if request.method == 'POST':
        telegram_id = request.form.get('telegram_id', '').strip()
        if not telegram_id:
            flash('❌ Введите Telegram ID', 'error')
            return redirect(url_for('setup_2fa'))
        conn.execute('INSERT OR REPLACE INTO user_2fa (user_id, telegram_id, enabled, secret) VALUES (?, ?, 1, ?)',
                     (session['user_id'], telegram_id, secrets.token_hex(16)))
        conn.commit()
        flash('✅ 2FA настроена', 'success')
        return redirect(url_for('index'))
    user_2fa = conn.execute('SELECT telegram_id, enabled FROM user_2fa WHERE user_id = ?', (session['user_id'],)).fetchone()
    return render_template('setup_2fa.html', user_2fa=user_2fa)

@app.route('/disable_2fa', methods=['POST'])
@login_required
def disable_2fa():
    conn = get_db()
    conn.execute('DELETE FROM user_2fa WHERE user_id = ?', (session['user_id'],))
    conn.commit()
    flash('✅ 2FA отключена', 'success')
    return redirect(url_for('index'))

@app.route('/verify_2fa', methods=['GET', 'POST'])
def verify_2fa():
    if 'pending_user_id' not in session:
        return redirect(url_for('login'))
    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        if verify_login_code(session['pending_user_id'], code):
            session['user_id'] = session['pending_user_id']
            session['is_admin'] = session.get('pending_is_admin', False)
            session.pop('pending_user_id', None)
            session.pop('pending_is_admin', None)
            flash('✅ Добро пожаловать!', 'success')
            return redirect(url_for('index'))
        else:
            flash('❌ Неверный код', 'error')
    return render_template('verify_2fa.html')

@app.route(f'/{ADMIN_SECRET}/dashboard')
@admin_required
def admin_dashboard():
    conn = get_db()
    total_users = conn.execute('SELECT COUNT(*) as count FROM users').fetchone()['count']
    total_balance = conn.execute('SELECT SUM(farm_balance) as total FROM users').fetchone()['total'] or 0
    total_deposits = conn.execute('SELECT SUM(amount) as total FROM deposit_requests WHERE status = "confirmed"').fetchone()['total'] or 0
    total_withdraws_pending = conn.execute('SELECT COUNT(*) as count FROM withdraw_requests WHERE status = "pending"').fetchone()['count']
    total_deposits_pending = conn.execute('SELECT COUNT(*) as count FROM deposit_requests WHERE status = "pending"').fetchone()['count']
    week_ago = time.time() - (7 * 86400)
    new_users_week = conn.execute('SELECT COUNT(*) as count FROM users WHERE created_at > ?', (week_ago,)).fetchone()['count']
    total_storage_items = conn.execute('SELECT SUM(quantity) as total FROM storage').fetchone()['total'] or 0
    total_referrals = conn.execute('SELECT COUNT(*) as count FROM referral_history').fetchone()['count']

    stats = {
        'total_users': total_users,
        'total_balance': f"{total_balance:.2f}",
        'total_deposits': f"{total_deposits:.2f}",
        'total_withdraws_pending': total_withdraws_pending,
        'total_deposits_pending': total_deposits_pending,
        'new_users_week': new_users_week,
        'total_storage_items': f"{total_storage_items:.4f}",
        'total_referrals': total_referrals
    }
    return render_template('admin/dashboard.html', stats=stats, admin_secret=ADMIN_SECRET)

def get_withdraw_flags(user_id, wallet_address, amount):
    conn = get_db()
    flags = []
    
    same_wallet_count = conn.execute(
        'SELECT COUNT(*) FROM withdraw_requests WHERE wallet_address = ? AND status = "completed"',
        (wallet_address,)
    ).fetchone()[0]
    if same_wallet_count >= 3:
        flags.append('⚠️ На этот кошелек выводили уже 3+ аккаунта')
    
    user = conn.execute('SELECT bonus_balance, farm_balance FROM users WHERE id = ?', (user_id,)).fetchone()
    if user and user['bonus_balance'] > user['farm_balance'] * 2 and user['farm_balance'] > 0:
        flags.append('⚠️ Бонусный баланс значительно превышает фермерский')
    
    user_data = conn.execute('SELECT created_at FROM users WHERE id = ?', (user_id,)).fetchone()
    if user_data and user_data['created_at']:
        days_old = (time.time() - user_data['created_at']) / 86400
        if days_old < 7 and amount > 100:
            flags.append('⚠️ Новый аккаунт (менее 7 дней) выводит >100 Coin')
    
    return flags

@app.route(f'/{ADMIN_SECRET}/withdraws')
@admin_required
def admin_withdraws():
    conn = get_db()
    withdraws = conn.execute('SELECT w.*, u.login, u.created_at, u.farm_balance FROM withdraw_requests w JOIN users u ON w.user_id = u.id ORDER BY w.created_at DESC').fetchall()

    withdraws_list = []
    for w in withdraws:
        total_deposits = conn.execute('SELECT SUM(amount) as total FROM deposit_requests WHERE user_id = ? AND status = "confirmed"', (w['user_id'],)).fetchone()['total'] or 0
        total_withdraws = conn.execute('SELECT SUM(amount) as total FROM withdraw_requests WHERE user_id = ? AND status = "completed"', (w['user_id'],)).fetchone()['total'] or 0
        days_on_project = 0
        if w['created_at']:
            days_on_project = int((time.time() - w['created_at']) / 86400)

        risk = 'low'
        risk_text = 'Низкий'
        if w['amount'] > 1000 and total_deposits * 100 < w['amount'] / 2:
            risk = 'high'
            risk_text = 'Высокий (вывод > пополнений)'
        elif w['amount'] > 500 and days_on_project < 7:
            risk = 'medium'
            risk_text = 'Средний (новый пользователь, крупный вывод)'
        elif w['amount'] > 5000:
            risk = 'medium'
            risk_text = 'Средний (очень крупный вывод)'
        
        flags = get_withdraw_flags(w['user_id'], w['wallet_address'], w['amount'])

        w_dict = dict(w)
        w_dict['user_stats'] = {
            'created_at': w['created_at'],
            'days_on_project': days_on_project,
            'total_deposits': total_deposits,
            'total_withdraws': total_withdraws,
            'balance': w['farm_balance'],
            'risk': risk,
            'risk_text': risk_text,
            'flags': flags
        }
        withdraws_list.append(w_dict)

    return render_template('admin/withdraws.html', withdraws=withdraws_list, admin_secret=ADMIN_SECRET)

@app.route(f'/{ADMIN_SECRET}/deposits')
@admin_required
def admin_deposits():
    conn = get_db()
    deposits = conn.execute('SELECT d.*, u.login FROM deposit_requests d JOIN users u ON d.user_id = u.id ORDER BY d.created_at DESC').fetchall()
    return render_template('admin/deposits.html', deposits=deposits, admin_secret=ADMIN_SECRET)

@app.route(f'/{ADMIN_SECRET}/users')
@admin_required
def admin_users():
    conn = get_db()
    users = conn.execute('SELECT id, login, balance, storage_level, grid_size, created_at, is_admin, bonus_balance, farm_balance FROM users ORDER BY farm_balance DESC').fetchall()
    return render_template('admin/users.html', users=users, admin_secret=ADMIN_SECRET)

@app.route(f'/{ADMIN_SECRET}/stats')
@admin_required
def admin_stats():
    conn = get_db()
    total_users = conn.execute('SELECT COUNT(*) as count FROM users').fetchone()['count']
    total_balance = conn.execute('SELECT SUM(farm_balance) as total FROM users').fetchone()['total'] or 0
    avg_balance = total_balance / total_users if total_users > 0 else 0
    crops_stats = conn.execute('SELECT crop, SUM(quantity) as total FROM storage WHERE crop IS NOT NULL GROUP BY crop ORDER BY total DESC').fetchall()
    garden_stats = conn.execute('SELECT COUNT(*) as total_cells, SUM(CASE WHEN crop IS NOT NULL THEN 1 ELSE 0 END) as planted FROM garden').fetchone()
    day_ago = time.time() - 86400
    week_ago = time.time() - (7 * 86400)
    month_ago = time.time() - (30 * 86400)
    new_users_day = conn.execute('SELECT COUNT(*) as count FROM users WHERE created_at > ?', (day_ago,)).fetchone()['count']
    new_users_week = conn.execute('SELECT COUNT(*) as count FROM users WHERE created_at > ?', (week_ago,)).fetchone()['count']
    new_users_month = conn.execute('SELECT COUNT(*) as count FROM users WHERE created_at > ?', (month_ago,)).fetchone()['count']
    deposits_total = conn.execute('SELECT SUM(amount) as total FROM deposit_requests WHERE status = "confirmed"').fetchone()['total'] or 0
    deposits_count = conn.execute('SELECT COUNT(*) as count FROM deposit_requests WHERE status = "confirmed"').fetchone()['count']
    withdraws_total = conn.execute('SELECT SUM(amount) as total FROM withdraw_requests WHERE status = "completed"').fetchone()['total'] or 0
    withdraws_count = conn.execute('SELECT COUNT(*) as count FROM withdraw_requests WHERE status = "completed"').fetchone()['count']

    stats = {
        'total_users': total_users,
        'total_balance': f"{total_balance:.2f}",
        'avg_balance': f"{avg_balance:.2f}",
        'crops_stats': crops_stats,
        'garden_stats': garden_stats,
        'new_users_day': new_users_day,
        'new_users_week': new_users_week,
        'new_users_month': new_users_month,
        'deposits_total': f"{deposits_total:.2f}",
        'deposits_count': deposits_count,
        'withdraws_total': f"{withdraws_total:.2f}",
        'withdraws_count': withdraws_count
    }
    return render_template('admin/stats.html', stats=stats, admin_secret=ADMIN_SECRET)

@app.route(f'/{ADMIN_SECRET}/process_withdraw/<int:withdraw_id>', methods=['POST'])
@admin_required
def process_withdraw(withdraw_id):
    action = request.form.get('action')
    conn = get_db()
    withdraw = conn.execute('SELECT user_id, amount FROM withdraw_requests WHERE id = ? AND status = "pending"', (withdraw_id,)).fetchone()
    if not withdraw:
        flash('❌ Заявка не найдена', 'error')
        return redirect(url_for('admin_withdraws'))
    if action == 'complete':
        conn.execute('UPDATE withdraw_requests SET status = "completed", processed_at = ? WHERE id = ?', (time.time(), withdraw_id))
        flash(f'✅ Заявка #{withdraw_id} выполнена', 'success')
    elif action == 'cancel':
        conn.execute('UPDATE users SET farm_balance = farm_balance + ? WHERE id = ?', (withdraw['amount'], withdraw['user_id']))
        conn.execute('UPDATE withdraw_requests SET status = "cancelled", processed_at = ? WHERE id = ?', (time.time(), withdraw_id))
        flash(f'⚠️ Заявка #{withdraw_id} отменена', 'warning')
    conn.commit()
    return redirect(url_for('admin_withdraws'))

@app.route(f'/{ADMIN_SECRET}/confirm_deposit/<int:deposit_id>', methods=['POST'])
@admin_required
def confirm_deposit(deposit_id):
    try:
        conn = get_db()

        deposit = conn.execute('SELECT user_id, amount, status FROM deposit_requests WHERE id = ?', (deposit_id,)).fetchone()

        if not deposit:
            flash('❌ Заявка не найдена', 'error')
            return redirect(url_for('admin_deposits'))

        if deposit['status'] != 'pending':
            flash(f'⚠️ Заявка уже обработана (статус: {deposit["status"]})', 'warning')
            return redirect(url_for('admin_deposits'))

        coins_amount = int(float(deposit['amount']) * 100)

        conn.execute('UPDATE users SET farm_balance = farm_balance + ? WHERE id = ?', (coins_amount, deposit['user_id']))

        unique_txid = f"manual_confirm_{deposit_id}_{int(time.time())}"

        conn.execute('UPDATE deposit_requests SET status = "confirmed", confirmed_at = ?, txid = ? WHERE id = ?',
                    (time.time(), unique_txid, deposit_id))

        conn.commit()

        flash(f'✅ Заявка #{deposit_id} подтверждена. Зачислено {coins_amount} Coin на фермерский баланс', 'success')

    except Exception as e:
        print(f"Ошибка в confirm_deposit: {e}")
        flash(f'❌ Ошибка: {str(e)}', 'error')

    return redirect(url_for('admin_deposits'))

@app.route(f'/{ADMIN_SECRET}/toggle_admin/<int:user_id>', methods=['POST'])
@admin_required
def toggle_admin(user_id):
    conn = get_db()
    user = conn.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,)).fetchone()
    if not user:
        flash('❌ Пользователь не найден', 'error')
        return redirect(url_for('admin_users'))
    new_status = 0 if user['is_admin'] else 1
    conn.execute('UPDATE users SET is_admin = ? WHERE id = ?', (new_status, user_id))
    conn.commit()
    flash(f'✅ Права администратора {"выданы" if new_status else "сняты"}', 'success')
    return redirect(url_for('admin_users'))

def migrate_passwords():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        users = conn.execute('SELECT id, password FROM users').fetchall()
        changed = 0
        for user in users:
            if len(user['password']) != 64:
                hashed = hash_password(user['password'])
                conn.execute('UPDATE users SET password = ? WHERE id = ?', (hashed, user['id']))
                changed += 1
        conn.commit()
        conn.close()
        if changed > 0:
            print(f"✅ Сконвертировано {changed} паролей")
    except Exception as e:
        print(f"Ошибка: {e}")

# ============= ЗАПУСК =============

init_db()
migrate_passwords()
start_transaction_monitor()

# Получаем реальный URL для админки
railway_url = os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'localhost:5000')
if railway_url != 'localhost:5000':
    admin_url = f"https://{railway_url}/{ADMIN_SECRET}/dashboard"
else:
    admin_url = f"http://{railway_url}/{ADMIN_SECRET}/dashboard"

print(f"\n" + "="*50)
print(f"🔐 АДМИН-ПАНЕЛЬ:")
print(f"   Секретный путь: {ADMIN_SECRET}")
print(f"   Ссылка: {admin_url}")
print("="*50 + "\n")

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
