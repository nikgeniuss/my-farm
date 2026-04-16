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
    'carrot': {'name': 'Морковь', 'cost': 10, 'income': 0.00000064, 'emoji': '🥕', 'color': '#FFA500'},
    'potato': {'name': 'Картофель', 'cost': 50, 'income': 0.00000350, 'emoji': '🥔', 'color': '#D2691E'},
    'onion': {'name': 'Лук', 'cost': 120, 'income': 0.00000926, 'emoji': '🧅', 'color': '#C41E3A'},
    'cabbage': {'name': 'Капуста', 'cost': 300, 'income': 0.0000257, 'emoji': '🥬', 'color': '#2E8B57'},
    'tomato': {'name': 'Томат', 'cost': 800, 'income': 0.0000772, 'emoji': '🍅', 'color': '#FF4500'},
    'cucumber': {'name': 'Огурец', 'cost': 1500, 'income': 0.000152, 'emoji': '🥒', 'color': '#3CB371'},
    'pepper': {'name': 'Перец', 'cost': 3000, 'income': 0.000330, 'emoji': '🫑', 'color': '#32CD32'},
    'eggplant': {'name': 'Баклажан', 'cost': 7000, 'income': 0.000818, 'emoji': '🍆', 'color': '#800080'},
    'corn': {'name': 'Кукуруза', 'cost': 15000, 'income': 0.00186, 'emoji': '🌽', 'color': '#FFD700'},
    'watermelon': {'name': 'Арбуз', 'cost': 30000, 'income': 0.00386, 'emoji': '🍉', 'color': '#32CD32'}
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
    seconds_in_3_5_months = 9072000  # 3.5 месяца (105 дней)
    price = (income_increase * seconds_in_3_5_months) / price_factor
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
        try:
            conn.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0")
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

        # ============= ТАБЛИЦЫ ДЛЯ ЗАДАНИЙ =============
        conn.execute('''CREATE TABLE IF NOT EXISTS quest_templates
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         quest_type TEXT,
                         quest_key TEXT UNIQUE,
                         name TEXT,
                         description TEXT,
                         target INTEGER,
                         reward INTEGER,
                         reward_type TEXT DEFAULT 'coins',
                         extra_data TEXT,
                         is_active INTEGER DEFAULT 1)''')

        conn.execute('''CREATE TABLE IF NOT EXISTS user_quests
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         quest_key TEXT,
                         progress INTEGER DEFAULT 0,
                         completed INTEGER DEFAULT 0,
                         claimed INTEGER DEFAULT 0,
                         created_at REAL,
                         expires_at REAL,
                         FOREIGN KEY (user_id) REFERENCES users(id))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS user_achievements
                        (user_id INTEGER,
                         achievement_key TEXT,
                         progress INTEGER DEFAULT 0,
                         completed INTEGER DEFAULT 0,
                         claimed INTEGER DEFAULT 0,
                         completed_at REAL,
                         PRIMARY KEY (user_id, achievement_key))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS user_chain_quests
                        (user_id INTEGER,
                         chain_key TEXT,
                         current_step INTEGER DEFAULT 0,
                         claimed_steps TEXT DEFAULT '[]',
                         PRIMARY KEY (user_id, chain_key))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS user_social_quests
                        (user_id INTEGER,
                         quest_key TEXT,
                         status TEXT DEFAULT 'pending',
                         completed_at REAL,
                         claimed INTEGER DEFAULT 0,
                         PRIMARY KEY (user_id, quest_key))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS user_season_pass
                        (user_id INTEGER,
                         season_id INTEGER,
                         premium INTEGER DEFAULT 0,
                         xp INTEGER DEFAULT 0,
                         level INTEGER DEFAULT 1,
                         claimed_free TEXT DEFAULT '[]',
                         claimed_premium TEXT DEFAULT '[]',
                         PRIMARY KEY (user_id, season_id))''')

        conn.execute('''CREATE TABLE IF NOT EXISTS season_config
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         season_id INTEGER,
                         name TEXT,
                         starts_at REAL,
                         ends_at REAL,
                         is_active INTEGER DEFAULT 1,
                         premium_cost INTEGER DEFAULT 500)''')

        # Начальные данные для заданий
        quest_count = conn.execute('SELECT COUNT(*) FROM quest_templates').fetchone()[0]
        if quest_count == 0:
            # Ежедневные
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('daily', 'daily_plant_3', '🌱 Посадка', 'Посадить 3 любых овоща', 3, 3)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('daily', 'daily_sell_10', '💰 Продажи', 'Продать 10 овощей', 10, 5)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('daily', 'daily_upgrade_1', '⚡ Апгрейд', 'Купить 1 апгрейд', 1, 5)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('daily', 'daily_harvest_50', '📦 Сбор урожая', 'Собрать 50 овощей со склада', 50, 10)''')
            
            # Еженедельные
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('weekly', 'weekly_plant_20', '🌱 Массовая посадка', 'Посадить 20 овощей', 20, 20)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('weekly', 'weekly_sell_100', '💰 Крупные продажи', 'Продать 100 овощей', 100, 30)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('weekly', 'weekly_upgrade_5', '⚡ Мастер апгрейдов', 'Купить 5 апгрейдов', 5, 50)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('weekly', 'weekly_expand_1', '🌾 Расширение', 'Расширить огород', 1, 100)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('weekly', 'weekly_storage_1', '🏚️ Склад', 'Улучшить склад', 1, 75)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('weekly', 'weekly_earn_500', '💵 Доход', 'Заработать 500 Coin с огорода', 500, 100)''')
            
            # Достижения
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_first_plant', '🌱 Первый шаг', 'Посадить первый овощ', 1, 10)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_first_sell', '📦 Первый урожай', 'Продать 10 овощей', 10, 15)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_upgrade_10', '⚡ Мастер апгрейдов', 'Купить 10 апгрейдов', 10, 50)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_storage_5', '🏚️ Складской маг', 'Улучшить склад до 5 уровня', 5, 100)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_garden_5', '🌾 Фермер', 'Расширить огород до 5x5', 5, 150)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_all_vegs', '🌽 Коллекционер', 'Посадить все 10 видов овощей', 10, 200)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_referrals_10', '👥 Популярный', 'Пригласить 10 друзей', 10, 300)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_deposit_50', '💰 Инвестор', 'Пополнить на 50 USDT', 50, 500)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_watermelon', '🏆 Легенда', 'Купить Арбуз', 1, 1000)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('achievement', 'ach_garden_6', '⭐ Максималист', 'Расширить огород до 6x6', 6, 2000)''')
            
            # Социальные
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('social', 'social_tg_channel', '📱 Telegram канал', 'Подписаться на Telegram канал', 1, 20)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('social', 'social_tg_chat', '💬 Telegram чат', 'Подписаться на Telegram чат', 1, 20)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('social', 'social_discord', '🎮 Discord', 'Вступить в Discord сервер', 1, 20)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('social', 'social_youtube', '▶️ YouTube', 'Подписаться на YouTube', 1, 20)''')
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward)
                            VALUES ('social', 'social_review', '📝 Отзыв', 'Написать отзыв о проекте', 1, 50)''')
            
            # Цепочка овощей
            conn.execute('''INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward, extra_data)
                            VALUES ('chain', 'chain_vegetables', '🌽 Цепочка овощей', 'Посади все овощи по порядку', 10, 0, 
                            '{"steps": ["carrot", "potato", "onion", "cabbage", "tomato", "cucumber", "pepper", "eggplant", "corn", "watermelon"],
                              "rewards": [2, 5, 10, 20, 40, 80, 150, 300, 600, 1200]}')''')
            
            # Сезон
            current_time = time.time()
            conn.execute('''INSERT INTO season_config (season_id, name, starts_at, ends_at, is_active, premium_cost)
                            VALUES (1, 'Сезон 1', ?, ?, 1, 500)''',
                         (current_time, current_time + 30 * 24 * 3600))

            conn.execute('''CREATE TABLE IF NOT EXISTS audit_log
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_id INTEGER,
                 user_login TEXT,
                 action TEXT,
                 table_name TEXT,
                 record_id INTEGER,
                 old_values TEXT,
                 new_values TEXT,
                 ip_address TEXT,
                 user_agent TEXT,
                 created_at REAL)''')

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

def audit_log(user_id, action, table_name=None, record_id=None, old_values=None, new_values=None):
    try:
        conn = get_db()
        user = conn.execute('SELECT login FROM users WHERE id = ?', (user_id,)).fetchone() if user_id else None
        user_login = user['login'] if user else 'system'
        
        conn.execute('''INSERT INTO audit_log 
                        (user_id, user_login, action, table_name, record_id, 
                         old_values, new_values, ip_address, user_agent, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (user_id, user_login, action, table_name, record_id,
                      json.dumps(old_values) if old_values else None,
                      json.dumps(new_values) if new_values else None,
                      request.remote_addr if request else None,
                      request.headers.get('User-Agent', '') if request else None,
                      time.time()))
        conn.commit()
    except Exception as e:
        print(f"Audit log error: {e}")

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
            
            # Обновляем прогресс заданий по сбору урожая
            total_harvested = sum(harvested_crops.values())
            if total_harvested > 0:
                update_quest_progress(user_id, 'daily', 'harvest', int(total_harvested))
            
            
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

def check_banned(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' in session:
            conn = get_db()
            user = conn.execute('SELECT is_banned FROM users WHERE id = ?', (session['user_id'],)).fetchone()
            if user and user['is_banned']:
                session.clear()
                flash('⛔ Ваш аккаунт заблокирован', 'error')
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
                grid_size, 
                storage_level, 
                bonus_balance, 
                farm_balance,
                register_ip,
                register_ua,
                device_hash,
                referrer_id,
                created_at,
                is_admin,
                is_banned
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

# ============= ФУНКЦИИ ДЛЯ ЗАДАНИЙ =============

def get_active_season():
    conn = get_db()
    now = time.time()
    season = conn.execute('''SELECT * FROM season_config 
                             WHERE is_active = 1 AND starts_at <= ? AND ends_at >= ?''',
                          (now, now)).fetchone()
    return dict(season) if season else None

def get_user_season_pass(user_id, season_id):
    conn = get_db()
    sp = conn.execute('SELECT * FROM user_season_pass WHERE user_id = ? AND season_id = ?',
                      (user_id, season_id)).fetchone()
    if not sp:
        conn.execute('''INSERT INTO user_season_pass (user_id, season_id, xp, level) 
                        VALUES (?, ?, 0, 1)''', (user_id, season_id))
        conn.commit()
        sp = conn.execute('SELECT * FROM user_season_pass WHERE user_id = ? AND season_id = ?',
                          (user_id, season_id)).fetchone()
    return dict(sp) if sp else None

def get_xp_for_level(level):
    xp_requirements = {
        1: 100, 2: 150, 3: 225, 4: 340, 5: 510,
        6: 765, 7: 1150, 8: 1725, 9: 2600
    }
    return xp_requirements.get(level, 0)

def add_season_xp(user_id, xp_amount):
    season = get_active_season()
    if not season:
        return
    
    sp = get_user_season_pass(user_id, season['season_id'])
    new_xp = sp['xp'] + xp_amount
    current_level = sp['level']
    
    while current_level < 10:
        xp_needed = get_xp_for_level(current_level)
        if new_xp >= xp_needed:
            new_xp -= xp_needed
            current_level += 1
        else:
            break
    
    conn = get_db()
    conn.execute('UPDATE user_season_pass SET xp = ?, level = ? WHERE user_id = ? AND season_id = ?',
                 (new_xp, current_level, user_id, season['season_id']))
    conn.commit()
    
    return current_level

def update_quest_progress(user_id, quest_type, action, value=1):
    conn = get_db()
    now = time.time()
    
    if quest_type in ['daily', 'weekly']:
        templates = conn.execute('''SELECT * FROM quest_templates 
                                    WHERE quest_type = ? AND is_active = 1''',
                                 (quest_type,)).fetchall()
        
        for tpl in templates:
            tpl = dict(tpl)
            
            if action not in tpl['quest_key']:
                continue
            
            user_quest = conn.execute('''SELECT * FROM user_quests 
                                         WHERE user_id = ? AND quest_key = ?''',
                                      (user_id, tpl['quest_key'])).fetchone()
            
            if not user_quest:
                if quest_type == 'daily':
                    expires_at = now + 86400
                else:
                    days_to_monday = 7 - datetime.now().weekday()
                    if days_to_monday == 7:
                        days_to_monday = 0
                    expires_at = now + days_to_monday * 86400
                
                conn.execute('''INSERT INTO user_quests (user_id, quest_key, progress, created_at, expires_at) 
                                VALUES (?, ?, ?, ?, ?)''',
                             (user_id, tpl['quest_key'], value, now, expires_at))
            else:
                user_quest = dict(user_quest)
                if not user_quest['completed']:
                    new_progress = min(user_quest['progress'] + value, tpl['target'])
                    completed = 1 if new_progress >= tpl['target'] else 0
                    conn.execute('UPDATE user_quests SET progress = ?, completed = ? WHERE id = ?',
                                 (new_progress, completed, user_quest['id']))
    
    elif quest_type == 'achievement':
        templates = conn.execute('''SELECT * FROM quest_templates 
                                    WHERE quest_type = ? AND is_active = 1''',
                                 ('achievement',)).fetchall()
        
        for tpl in templates:
            tpl = dict(tpl)
            if action not in tpl['quest_key']:
                continue
            
            ach = conn.execute('SELECT * FROM user_achievements WHERE user_id = ? AND achievement_key = ?',
                               (user_id, tpl['quest_key'])).fetchone()
            
            if not ach:
                conn.execute('''INSERT INTO user_achievements (user_id, achievement_key, progress) 
                                VALUES (?, ?, ?)''',
                             (user_id, tpl['quest_key'], value))
            else:
                ach = dict(ach)
                if not ach['completed']:
                    new_progress = min(ach['progress'] + value, tpl['target'])
                    completed = 1 if new_progress >= tpl['target'] else 0
                    conn.execute('''UPDATE user_achievements 
                                    SET progress = ?, completed = ?, completed_at = ? 
                                    WHERE user_id = ? AND achievement_key = ?''',
                                 (new_progress, completed, now if completed else None,
                                  user_id, tpl['quest_key']))
    
    conn.commit()

def update_chain_quest(user_id, crop_key):
    conn = get_db()
    
    chain = conn.execute('''
        SELECT * FROM quest_templates 
        WHERE quest_type = 'chain' AND is_active = 1
    ''').fetchone()
    
    if not chain:
        conn.close()
        return
    
    chain = dict(chain)
    extra = json.loads(chain['extra_data'])
    steps = extra['steps']
    
    progress = conn.execute('''
        SELECT * FROM user_chain_quests 
        WHERE user_id = ? AND chain_key = ?
    ''', (user_id, chain['quest_key'])).fetchone()
    
    if not progress:
        conn.execute('''
            INSERT INTO user_chain_quests (user_id, chain_key, current_step)
            VALUES (?, ?, 0)
        ''', (user_id, chain['quest_key']))
        conn.commit()
        progress = conn.execute('''
            SELECT * FROM user_chain_quests 
            WHERE user_id = ? AND chain_key = ?
        ''', (user_id, chain['quest_key'])).fetchone()
    
    progress = dict(progress)
    current_step = progress['current_step']
    
    if current_step < len(steps) and steps[current_step] == crop_key:
        conn.execute('''
            UPDATE user_chain_quests 
            SET current_step = current_step + 1 
            WHERE user_id = ? AND chain_key = ?
        ''', (user_id, chain['quest_key']))
        conn.commit()
        
        if current_step + 1 == 10:
            update_quest_progress(user_id, 'achievement', 'all_vegs', 10)
    
    conn.close()

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

        # Обновляем прогресс заданий
        update_quest_progress(user_id, 'daily', 'login', 1)
        add_season_xp(user_id, 10)  # +10 XP за вход

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
@check_banned
@rate_limit(limit=60, window=60)
def index():
    user = get_user_with_stats(session['user_id'], skip_harvest=False)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))

    audit_log(session['user_id'], 'test_action')

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
            user = conn.execute('SELECT id, password, is_admin, is_banned FROM users WHERE login = ?', (login,)).fetchone()
            if not user:
                flash('❌ Пользователь не найден', 'error')
                return redirect(url_for('login'))
            if user['is_banned']:
                flash('⛔ Ваш аккаунт заблокирован', 'error')
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
            audit_log(user['id'], 'login_success')
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
            
            current_time = time.time()
            hashed_password = hash_password(password)
            
            cursor = conn.execute('''INSERT INTO users 
                (login, password, grid_size, storage_level, referrer_id, created_at, is_admin,
                 register_ip, register_ua, device_hash, bonus_balance, farm_balance, referrer_ip, referrer_ua) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (login, hashed_password, 3, 1, referrer_id, current_time, 0,
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
                update_quest_progress(referrer_id, 'achievement', 'referral', 1)
                add_season_xp(referrer_id, 50)  # +50 XP рефереру
                log_activity(login, 'referral', f'🎉 {login} зарегистрировался по реферальной ссылке')
                flash('🎉 Аккаунт создан! Получено 200 Coin (бонусные)!', 'success')
            else:
                log_activity(login, 'register', f'🆕 {login} присоединился к игре')
                flash('🎉 Аккаунт создан! В подарок 100 Coin (бонусные)!', 'success')
            
            conn.commit()
            session['user_id'] = user_id
            session['is_admin'] = 0
            audit_log(user_id, 'register', 'users', user_id, None, {'login': login})
            return redirect(url_for('index'))
    referrer_id = request.args.get('ref')
    return render_template('login.html', referrer_id=referrer_id)

@app.route('/profile')
@login_required
@check_banned
@rate_limit(limit=30, window=60)
def profile():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    
    conn = get_db()
    
    # Сумма пополнений
    total_deposits = conn.execute('''
        SELECT SUM(amount) as total FROM deposit_requests 
        WHERE user_id = ? AND status = 'confirmed'
    ''', (session['user_id'],)).fetchone()['total'] or 0
    
    # Количество пополнений
    deposits_count = conn.execute('''
        SELECT COUNT(*) as count FROM deposit_requests 
        WHERE user_id = ? AND status = 'confirmed'
    ''', (session['user_id'],)).fetchone()['count'] or 0
    
    # Сумма выводов
    total_withdraws = conn.execute('''
        SELECT SUM(amount) as total FROM withdraw_requests 
        WHERE user_id = ? AND status = 'completed'
    ''', (session['user_id'],)).fetchone()['total'] or 0
    
    # Количество выводов
    withdraws_count = conn.execute('''
        SELECT COUNT(*) as count FROM withdraw_requests 
        WHERE user_id = ? AND status = 'completed'
    ''', (session['user_id'],)).fetchone()['count'] or 0
    
    # Сумма в процессе вывода (pending)
    pending_withdraws = conn.execute('''
        SELECT SUM(amount) as total FROM withdraw_requests 
        WHERE user_id = ? AND status = 'pending'
    ''', (session['user_id'],)).fetchone()['total'] or 0
    
    conn.close()
    
    return render_template('profile.html', 
                          user=user,
                          total_deposits=total_deposits,
                          deposits_count=deposits_count,
                          total_withdraws=total_withdraws,
                          withdraws_count=withdraws_count,
                          pending_withdraws=pending_withdraws,
                          income_per_sec=user['income_per_sec'],
                          income_per_hour=user['income_per_hour'],
                          income_per_day=user['income_per_day'],
                          income_per_month=user['income_per_month'])

@app.route('/logout')
def logout():
    audit_log(session.get('user_id'), 'logout')
    session.clear()
    flash('👋 До свидания!', 'info')
    return redirect(url_for('login'))

@app.route('/plant/<int:cell_id>', methods=['POST'])
@login_required
@check_banned
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

        update_quest_progress(session['user_id'], 'daily', 'plant', 1)
        update_quest_progress(session['user_id'], 'weekly', 'plant', 1)
        update_quest_progress(session['user_id'], 'achievement', 'plant', 1)
        update_chain_quest(session['user_id'], crop_key)
        add_season_xp(session['user_id'], 10)  # +10 XP за посадку

        user_login = conn.execute('SELECT login FROM users WHERE id = ?', (session['user_id'],)).fetchone()['login']
        log_activity(user_login, 'plant', f'🌱 {user_login} посадил {crop_name}')
        flash(f'✅ {crop_name} посажен! -{crop_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {crop_cost} Coin', 'error')

    return redirect(url_for('index'))

@app.route('/upgrade/<int:cell_id>', methods=['POST'])
@login_required
@check_banned
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

        update_quest_progress(session['user_id'], 'daily', 'upgrade', 1)
        update_quest_progress(session['user_id'], 'weekly', 'upgrade', 1)
        update_quest_progress(session['user_id'], 'achievement', 'upgrade', 1)
        add_season_xp(session['user_id'], 20)  # +20 XP за апгрейд
        log_activity(upgrade_user_login, 'upgrade', f'⚡ {upgrade_user_login} купил апгрейд {upgrade_name}')
        flash(f'✨ {upgrade_name} куплен! +{upgrade_multiplier*100:.0f}%! -{upgrade_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {upgrade_cost} Coin', 'error')
    return redirect(url_for('index'))

@app.route('/expand_garden', methods=['POST'])
@login_required
@check_banned
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
        update_quest_progress(session['user_id'], 'weekly', 'expand', 1)
        update_quest_progress(session['user_id'], 'achievement', 'garden', new_size)
        add_season_xp(session['user_id'], 50)  # +50 XP за расширение
        log_activity(expand_user_login, 'expand', f'🌾 {expand_user_login} расширил огород до {new_size}x{new_size}')
        flash(f'🌾 Огород расширен до {new_size}x{new_size}! -{expand_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {expand_cost} Coin', 'error')
    return redirect(url_for('index'))

@app.route('/storage')
@login_required
@check_banned
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
@check_banned
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

    update_quest_progress(session['user_id'], 'daily', 'sell', int(quantity))
    update_quest_progress(session['user_id'], 'weekly', 'sell', int(quantity))
    update_quest_progress(session['user_id'], 'achievement', 'sell', int(quantity))
    
    flash(f'💰 Продано {quantity:.8f} {VEGETABLES[crop]["name"]} за {total_earned:.8f} Coin (зачислено на фермерский баланс)', 'success')
    return redirect(url_for('storage'))

@app.route('/sell_all/<crop>', methods=['POST'])
@login_required
@check_banned
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

    update_quest_progress(session['user_id'], 'daily', 'sell', int(quantity))
    update_quest_progress(session['user_id'], 'weekly', 'sell', int(quantity))
    update_quest_progress(session['user_id'], 'achievement', 'sell', int(quantity))
    
    flash(f'💰 Продано всё {VEGETABLES[crop]["name"]} ({quantity:.8f} шт) за {total_earned:.8f} Coin (зачислено на фермерский баланс)', 'success')
    return redirect(url_for('storage'))

@app.route('/sell_all_storage', methods=['POST'])
@login_required
@check_banned
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
    
    flash(f'💰 Продано всё ({total_items:.8f} шт) за {total_earned:.8f} Coin (зачислено на фермерский баланс)', 'success')
    return redirect(url_for('storage'))

@app.route('/upgrade_storage', methods=['POST'])
@login_required
@check_banned
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
        update_quest_progress(session['user_id'], 'weekly', 'storage', 1)
        update_quest_progress(session['user_id'], 'achievement', 'storage', current_level + 1)
        add_season_xp(session['user_id'], 30)  # +30 XP за улучшение склада
        flash(f'🏚️ Склад улучшен до {current_level + 1} уровня! Вместимость: {new_capacity:.0f} шт. -{upgrade_cost} Coin', 'success')
    else:
        flash(f'❌ Недостаточно средств! Нужно {upgrade_cost} Coin', 'error')
    return redirect(url_for('storage'))

@app.route('/referrals')
@login_required
@check_banned
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
@check_banned
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
@check_banned
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
@check_banned
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
@check_banned
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
@check_banned
@rate_limit(limit=30, window=60)
def about():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    return render_template('about.html', user=user, income_per_sec=user['income_per_sec'], income_per_hour=user['income_per_hour'], income_per_day=user['income_per_day'], income_per_month=user['income_per_month'])

@app.route('/leaderboard')
@login_required
@check_banned
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

@app.route('/wallet')
@login_required
@check_banned
@rate_limit(limit=30, window=60)
def wallet():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    
    conn = get_db()
    deposits = conn.execute('SELECT id, amount, memo, status, created_at FROM deposit_requests WHERE user_id = ? ORDER BY created_at DESC', 
                           (session['user_id'],)).fetchall()
    withdraws = conn.execute('SELECT id, amount, wallet_address, status, created_at FROM withdraw_requests WHERE user_id = ? ORDER BY created_at DESC',
                            (session['user_id'],)).fetchall()
    conn.close()
    
    return render_template('wallet.html',
                          user=user,
                          deposits=deposits,
                          withdraws=withdraws,
                          wallet_address=USDT_TON_WALLET,
                          income_per_sec=user['income_per_sec'],
                          income_per_hour=user['income_per_hour'],
                          income_per_day=user['income_per_day'],
                          income_per_month=user['income_per_month'])

@app.route('/quests')
@login_required
@check_banned
@rate_limit(limit=30, window=60)
def quests():
    user = get_user_with_stats(session['user_id'], skip_harvest=True)
    if not user:
        flash('❌ Ошибка загрузки данных', 'error')
        return redirect(url_for('logout'))
    
    conn = get_db()
    now = time.time()
    
    # Активный сезон
    season = get_active_season()
    season_pass = None
    if season:
        season_pass = get_user_season_pass(session['user_id'], season['season_id'])
    
    # Ежедневные задания
    daily_quests = conn.execute('''
        SELECT qt.*, uq.progress, uq.completed, uq.claimed 
        FROM quest_templates qt
        LEFT JOIN user_quests uq ON qt.quest_key = uq.quest_key AND uq.user_id = ?
        WHERE qt.quest_type = 'daily' AND qt.is_active = 1
    ''', (session['user_id'],)).fetchall()
    
    # Еженедельные задания
    weekly_quests = conn.execute('''
        SELECT qt.*, uq.progress, uq.completed, uq.claimed 
        FROM quest_templates qt
        LEFT JOIN user_quests uq ON qt.quest_key = uq.quest_key AND uq.user_id = ?
        WHERE qt.quest_type = 'weekly' AND qt.is_active = 1
    ''', (session['user_id'],)).fetchall()
    
    # Достижения
    achievements = conn.execute('''
        SELECT qt.*, ua.completed, ua.claimed, ua.completed_at
        FROM quest_templates qt
        LEFT JOIN user_achievements ua ON qt.quest_key = ua.achievement_key AND ua.user_id = ?
        WHERE qt.quest_type = 'achievement' AND qt.is_active = 1
        ORDER BY qt.id
    ''', (session['user_id'],)).fetchall()
    
    # Цепочка овощей
    chain_quest = conn.execute('''
        SELECT * FROM quest_templates WHERE quest_type = 'chain' AND is_active = 1
    ''').fetchone()
    
    if chain_quest:
        chain_quest = dict(chain_quest)
        if chain_quest['extra_data']:
            chain_quest['extra_data'] = json.loads(chain_quest['extra_data'])
    
    chain_progress = None
    if chain_quest:
        chain_progress = conn.execute('''
            SELECT * FROM user_chain_quests WHERE user_id = ? AND chain_key = ?
        ''', (session['user_id'], chain_quest['quest_key'])).fetchone()
        
        if not chain_progress:
            conn.execute('''
                INSERT INTO user_chain_quests (user_id, chain_key, current_step)
                VALUES (?, ?, 0)
            ''', (session['user_id'], chain_quest['quest_key']))
            conn.commit()
            chain_progress = conn.execute('''
                SELECT * FROM user_chain_quests WHERE user_id = ? AND chain_key = ?
            ''', (session['user_id'], chain_quest['quest_key'])).fetchone()
    
    # Социальные задания
    social_quests = conn.execute('''
        SELECT qt.*, usq.status, usq.claimed
        FROM quest_templates qt
        LEFT JOIN user_social_quests usq ON qt.quest_key = usq.quest_key AND usq.user_id = ?
        WHERE qt.quest_type = 'social' AND qt.is_active = 1
    ''', (session['user_id'],)).fetchall()
    
    # Статистика
    daily_total = len(daily_quests)
    daily_completed = sum(1 for q in daily_quests if q['completed'])
    weekly_total = len(weekly_quests)
    weekly_completed = sum(1 for q in weekly_quests if q['completed'])
    achievements_total = len(achievements)
    achievements_completed = sum(1 for a in achievements if a['completed'])
    
    conn.close()
    
    return render_template('quests.html',
                          user=user,
                          season=season,
                          season_pass=season_pass,
                          daily_quests=daily_quests,
                          weekly_quests=weekly_quests,
                          achievements=achievements,
                          chain_quest=chain_quest,
                          chain_progress=chain_progress,
                          social_quests=social_quests,
                          daily_total=daily_total,
                          daily_completed=daily_completed,
                          weekly_total=weekly_total,
                          weekly_completed=weekly_completed,
                          achievements_total=achievements_total,
                          achievements_completed=achievements_completed,
                          now=now,
                          vegetables=VEGETABLES,
                          income_per_sec=user['income_per_sec'],
                          income_per_hour=user['income_per_hour'],
                          income_per_day=user['income_per_day'],
                          income_per_month=user['income_per_month'])

@app.route('/claim_quest_reward', methods=['POST'])
@login_required
@check_banned
def claim_quest_reward():
    data = request.get_json()
    quest_type = data.get('quest_type')
    quest_key = data.get('quest_key')
    
    if not quest_type or not quest_key:
        return jsonify({'success': False, 'error': 'Неверные данные'})
    
    conn = get_db()
    user_id = session['user_id']
    reward = 0
    
    try:
        if quest_type in ['daily', 'weekly']:
            quest = conn.execute('''
                SELECT * FROM user_quests WHERE user_id = ? AND quest_key = ? AND completed = 1 AND claimed = 0
            ''', (user_id, quest_key)).fetchone()
            
            if not quest:
                return jsonify({'success': False, 'error': 'Задание не выполнено или награда уже получена'})
            
            template = conn.execute('SELECT reward FROM quest_templates WHERE quest_key = ?', (quest_key,)).fetchone()
            reward = template['reward']
            
            conn.execute('UPDATE user_quests SET claimed = 1 WHERE id = ?', (quest['id'],))
            conn.execute('UPDATE users SET bonus_balance = bonus_balance + ? WHERE id = ?', (reward, user_id))
            
            # Бонус за все ежедневные/еженедельные
            if quest_type == 'daily':
                all_daily = conn.execute('''
                    SELECT COUNT(*) as total FROM quest_templates WHERE quest_type = 'daily' AND is_active = 1
                ''').fetchone()['total']
                claimed_daily = conn.execute('''
                    SELECT COUNT(*) as total FROM user_quests WHERE user_id = ? AND claimed = 1
                ''', (user_id,)).fetchone()['total']
                
                if claimed_daily == all_daily:
                    conn.execute('UPDATE users SET bonus_balance = bonus_balance + 30 WHERE id = ?', (user_id,))
                    reward += 30
            
            elif quest_type == 'weekly':
                all_weekly = conn.execute('''
                    SELECT COUNT(*) as total FROM quest_templates WHERE quest_type = 'weekly' AND is_active = 1
                ''').fetchone()['total']
                claimed_weekly = conn.execute('''
                    SELECT COUNT(*) as total FROM user_quests WHERE user_id = ? AND claimed = 1
                ''', (user_id,)).fetchone()['total']
                
                if claimed_weekly == all_weekly:
                    conn.execute('UPDATE users SET bonus_balance = bonus_balance + 200 WHERE id = ?', (user_id,))
                    reward += 200
        
        elif quest_type == 'achievement':
            ach = conn.execute('''
                SELECT * FROM user_achievements WHERE user_id = ? AND achievement_key = ? AND completed = 1 AND claimed = 0
            ''', (user_id, quest_key)).fetchone()
            
            if not ach:
                return jsonify({'success': False, 'error': 'Достижение не выполнено или награда уже получена'})
            
            template = conn.execute('SELECT reward FROM quest_templates WHERE quest_key = ?', (quest_key,)).fetchone()
            reward = template['reward']
            
            conn.execute('UPDATE user_achievements SET claimed = 1 WHERE user_id = ? AND achievement_key = ?', (user_id, quest_key))
            conn.execute('UPDATE users SET bonus_balance = bonus_balance + ? WHERE id = ?', (reward, user_id))
        
        elif quest_type == 'chain':
            data = request.get_json()
            step = data.get('step')
            
            chain = conn.execute('SELECT * FROM quest_templates WHERE quest_key = ?', (quest_key,)).fetchone()
            if not chain:
                return jsonify({'success': False, 'error': 'Цепочка не найдена'})
            
            extra = json.loads(chain['extra_data'])
            rewards = extra['rewards']
            
            if step < 1 or step > len(rewards):
                return jsonify({'success': False, 'error': 'Неверный шаг'})
            
            progress = conn.execute('''
                SELECT * FROM user_chain_quests WHERE user_id = ? AND chain_key = ?
            ''', (user_id, quest_key)).fetchone()
            
            claimed_steps = json.loads(progress['claimed_steps']) if progress else []
            
            if step in claimed_steps:
                return jsonify({'success': False, 'error': 'Награда уже получена'})
            
            if progress['current_step'] < step:
                return jsonify({'success': False, 'error': 'Шаг ещё не достигнут'})
            
            reward = rewards[step - 1]
            claimed_steps.append(step)
            
            conn.execute('UPDATE user_chain_quests SET claimed_steps = ? WHERE user_id = ? AND chain_key = ?',
                        (json.dumps(claimed_steps), user_id, quest_key))
            conn.execute('UPDATE users SET bonus_balance = bonus_balance + ? WHERE id = ?', (reward, user_id))
        
        elif quest_type == 'season':
            level = data.get('level')
            is_premium = data.get('is_premium', False)
            
            season = get_active_season()
            if not season:
                return jsonify({'success': False, 'error': 'Нет активного сезона'})
            
            sp = get_user_season_pass(user_id, season['season_id'])
            
            if sp['level'] < level:
                return jsonify({'success': False, 'error': 'Уровень не достигнут'})
            
            claimed_key = 'claimed_premium' if is_premium else 'claimed_free'
            claimed = json.loads(sp[claimed_key])
            
            if level in claimed:
                return jsonify({'success': False, 'error': 'Награда уже получена'})
            
            if is_premium and not sp['premium']:
                return jsonify({'success': False, 'error': 'Премиум не куплен'})
            
            # Награды по уровням
            free_rewards = {1: 5, 2: 10, 3: 15, 4: 20, 5: 25, 6: 30, 7: 35, 8: 40, 9: 50, 10: 100}
            premium_rewards = {1: 15, 2: 30, 3: 45, 4: 60, 5: 75, 6: 90, 7: 105, 8: 120, 9: 150, 10: 300}
            
            reward = premium_rewards[level] if is_premium else free_rewards[level]
            
            claimed.append(level)
            conn.execute(f'UPDATE user_season_pass SET {claimed_key} = ? WHERE user_id = ? AND season_id = ?',
                        (json.dumps(claimed), user_id, season['season_id']))
            conn.execute('UPDATE users SET bonus_balance = bonus_balance + ? WHERE id = ?', (reward, user_id))
        
        else:
            return jsonify({'success': False, 'error': 'Неизвестный тип задания'})
        
        conn.commit()
        
        # Логируем
        user_login = conn.execute('SELECT login FROM users WHERE id = ?', (user_id,)).fetchone()['login']
        log_activity(user_login, 'quest', f'🎯 {user_login} получил награду за задание: {quest_key}')
        
        return jsonify({'success': True, 'reward': reward})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)})
    finally:
        conn.close()

@app.route('/buy_premium_pass', methods=['POST'])
@login_required
@check_banned
def buy_premium_pass():
    season = get_active_season()
    if not season:
        return jsonify({'success': False, 'error': 'Нет активного сезона'})
    
    conn = get_db()
    user_id = session['user_id']
    
    sp = get_user_season_pass(user_id, season['season_id'])
    
    if sp['premium']:
        return jsonify({'success': False, 'error': 'Премиум уже куплен'})
    
    user = conn.execute('SELECT bonus_balance, farm_balance FROM users WHERE id = ?', (user_id,)).fetchone()
    cost = season['premium_cost']
    
    total_balance = user['bonus_balance'] + user['farm_balance']
    
    if total_balance < cost:
        return jsonify({'success': False, 'error': 'Недостаточно средств'})
    
    # Списываем средства
    if user['bonus_balance'] >= cost:
        conn.execute('UPDATE users SET bonus_balance = bonus_balance - ? WHERE id = ?', (cost, user_id))
    else:
        remaining = cost - user['bonus_balance']
        conn.execute('UPDATE users SET bonus_balance = 0, farm_balance = farm_balance - ? WHERE id = ?', (remaining, user_id))
    
    conn.execute('UPDATE user_season_pass SET premium = 1 WHERE user_id = ? AND season_id = ?',
                (user_id, season['season_id']))
    conn.commit()
    
    user_login = conn.execute('SELECT login FROM users WHERE id = ?', (user_id,)).fetchone()['login']
    log_activity(user_login, 'season_pass', f'⭐ {user_login} купил премиум-пропуск')
    
    return jsonify({'success': True, 'message': 'Премиум-пропуск активирован!'})

@app.route('/check_social_quest', methods=['POST'])
@login_required
@check_banned
def check_social_quest():
    data = request.get_json()
    quest_key = data.get('quest_key')
    
    conn = get_db()
    user_id = session['user_id']
    
    # Здесь должна быть реальная проверка через API соцсетей
    # Пока просто помечаем как ожидающее подтверждения
    conn.execute('''
        INSERT OR REPLACE INTO user_social_quests (user_id, quest_key, status)
        VALUES (?, ?, 'pending')
    ''', (user_id, quest_key))
    conn.commit()
    
    return jsonify({'success': True, 'message': 'Заявка отправлена на проверку'})



@app.route('/claim_daily_bonus', methods=['POST'])
@login_required
@check_banned
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
@check_banned
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
@check_banned
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
@check_banned
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
@check_banned
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
    users = conn.execute('''
        SELECT id, login, farm_balance, bonus_balance, storage_level, grid_size, created_at, is_admin, is_banned 
        FROM users ORDER BY id DESC
    ''').fetchall()
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

@app.route(f'/{ADMIN_SECRET}/user/<int:user_id>')
@admin_required
def admin_user_detail(user_id):
    conn = get_db()
    user = conn.execute('''
        SELECT id, login, farm_balance, bonus_balance, storage_level, grid_size, 
               register_ip, register_ua, device_hash, referrer_id, created_at, 
               is_admin, is_banned
        FROM users WHERE id = ?
    ''', (user_id,)).fetchone()
    
    if not user:
        flash('❌ Пользователь не найден', 'error')
        return redirect(url_for('admin_users'))
    
    referrals = conn.execute('SELECT id, login, farm_balance, is_banned FROM users WHERE referrer_id = ?', (user_id,)).fetchall()
    referrals_tree = []
    for ref in referrals:
        children = conn.execute('SELECT id, login FROM users WHERE referrer_id = ?', (ref['id'],)).fetchall()
        referrals_tree.append({
            'id': ref['id'],
            'login': ref['login'],
            'balance': ref['farm_balance'],
            'is_banned': ref['is_banned'],
            'children': [{'id': c['id'], 'login': c['login']} for c in children]
        })
    
    return render_template('admin/user_detail.html', user=user, referrals_tree=referrals_tree, admin_secret=ADMIN_SECRET)

@app.route(f'/{ADMIN_SECRET}/update_balance/<int:user_id>', methods=['POST'])
@admin_required
def admin_update_balance(user_id):
    farm_balance = float(request.form.get('farm_balance', 0))
    bonus_balance = float(request.form.get('bonus_balance', 0))
    
    conn = get_db()
    conn.execute('UPDATE users SET farm_balance = ?, bonus_balance = ? WHERE id = ?', 
                (farm_balance, bonus_balance, user_id))
    conn.commit()
    
    flash('✅ Балансы обновлены', 'success')
    return redirect(url_for('admin_user_detail', user_id=user_id))

@app.route(f'/{ADMIN_SECRET}/change_password/<int:user_id>', methods=['POST'])
@admin_required
def admin_change_password(user_id):
    new_password = request.form.get('new_password')
    if not new_password or len(new_password) < 4:
        flash('❌ Пароль должен быть минимум 4 символа', 'error')
        return redirect(url_for('admin_user_detail', user_id=user_id))
    
    hashed = hash_password(new_password)
    conn = get_db()
    conn.execute('UPDATE users SET password = ? WHERE id = ?', (hashed, user_id))
    conn.commit()
    
    flash('✅ Пароль изменён', 'success')
    return redirect(url_for('admin_user_detail', user_id=user_id))

@app.route(f'/{ADMIN_SECRET}/toggle_ban/<int:user_id>', methods=['POST'])
@admin_required
def admin_toggle_ban(user_id):
    conn = get_db()
    user = conn.execute('SELECT is_banned FROM users WHERE id = ?', (user_id,)).fetchone()
    new_status = 0 if user['is_banned'] else 1
    conn.execute('UPDATE users SET is_banned = ? WHERE id = ?', (new_status, user_id))
    conn.commit()
    
    flash('✅ Статус блокировки изменён', 'success')
    return redirect(url_for('admin_user_detail', user_id=user_id))

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
        
@app.route(f'/{ADMIN_SECRET}/sql', methods=['GET', 'POST'])
@admin_required
def admin_sql():
    result = None
    error = None
    query = ''
    
    if request.method == 'POST':
        query = request.form.get('query', '').strip()
        if query:
            try:
                conn = get_db()
                cursor = conn.execute(query)
                
                # Проверяем что за запрос
                if query.lower().strip().startswith('select'):
                    # SELECT — показываем результат
                    rows = cursor.fetchall()
                    if rows:
                        columns = list(rows[0].keys())
                        result = {
                            'columns': columns,
                            'rows': [list(row) for row in rows]
                        }
                    else:
                        result = {'columns': [], 'rows': []}
                else:
                    # INSERT, UPDATE, DELETE — показываем сколько строк изменено
                    conn.commit()
                    result = {'affected': cursor.rowcount if hasattr(cursor, 'rowcount') else 'OK'}
                
            except Exception as e:
                error = str(e)
    
    return render_template('admin/sql.html', 
                          admin_secret=ADMIN_SECRET, 
                          query=query, 
                          result=result, 
                          error=error)

# ============= АДМИНКА ЗАДАНИЙ =============

@app.route(f'/{ADMIN_SECRET}/quests')
@admin_required
def admin_quests():
    conn = get_db()
    quests = conn.execute('''
        SELECT * FROM quest_templates 
        ORDER BY 
            CASE quest_type 
                WHEN 'daily' THEN 1 
                WHEN 'weekly' THEN 2 
                WHEN 'chain' THEN 3 
                WHEN 'achievement' THEN 4 
                WHEN 'social' THEN 5 
            END, 
            id
    ''').fetchall()
    
    # Статистика
    stats = {
        'daily': conn.execute("SELECT COUNT(*) FROM quest_templates WHERE quest_type = 'daily'").fetchone()[0],
        'weekly': conn.execute("SELECT COUNT(*) FROM quest_templates WHERE quest_type = 'weekly'").fetchone()[0],
        'chain': conn.execute("SELECT COUNT(*) FROM quest_templates WHERE quest_type = 'chain'").fetchone()[0],
        'achievement': conn.execute("SELECT COUNT(*) FROM quest_templates WHERE quest_type = 'achievement'").fetchone()[0],
        'social': conn.execute("SELECT COUNT(*) FROM quest_templates WHERE quest_type = 'social'").fetchone()[0],
    }
    
    conn.close()
    
    return render_template('admin/quests.html', 
                          quests=quests, 
                          stats=stats, 
                          admin_secret=ADMIN_SECRET)


@app.route(f'/{ADMIN_SECRET}/quests/add', methods=['GET', 'POST'])
@admin_required
def admin_quest_add():
    if request.method == 'POST':
        quest_type = request.form.get('quest_type')
        quest_key = request.form.get('quest_key')
        name = request.form.get('name')
        description = request.form.get('description')
        target = int(request.form.get('target', 0))
        reward = int(request.form.get('reward', 0))
        extra_data = request.form.get('extra_data', '')
        
        if not all([quest_type, quest_key, name, target, reward]):
            flash('❌ Заполните все обязательные поля', 'error')
            return redirect(url_for('admin_quest_add'))
        
        conn = get_db()
        try:
            conn.execute('''
                INSERT INTO quest_templates (quest_type, quest_key, name, description, target, reward, extra_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (quest_type, quest_key, name, description, target, reward, extra_data or None))
            conn.commit()
            flash('✅ Задание добавлено', 'success')
            return redirect(url_for('admin_quests'))
        except sqlite3.IntegrityError:
            flash('❌ Задание с таким ключом уже существует', 'error')
        except Exception as e:
            flash(f'❌ Ошибка: {str(e)}', 'error')
        finally:
            conn.close()
    
    return render_template('admin/quest_add.html', admin_secret=ADMIN_SECRET)


@app.route(f'/{ADMIN_SECRET}/quests/edit/<int:quest_id>', methods=['GET', 'POST'])
@admin_required
def admin_quest_edit(quest_id):
    conn = get_db()
    
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        target = int(request.form.get('target', 0))
        reward = int(request.form.get('reward', 0))
        extra_data = request.form.get('extra_data', '')
        is_active = int(request.form.get('is_active', 1))
        
        conn.execute('''
            UPDATE quest_templates 
            SET name = ?, description = ?, target = ?, reward = ?, extra_data = ?, is_active = ?
            WHERE id = ?
        ''', (name, description, target, reward, extra_data or None, is_active, quest_id))
        conn.commit()
        conn.close()
        
        flash('✅ Задание обновлено', 'success')
        return redirect(url_for('admin_quests'))
    
    quest = conn.execute('SELECT * FROM quest_templates WHERE id = ?', (quest_id,)).fetchone()
    conn.close()
    
    if not quest:
        flash('❌ Задание не найдено', 'error')
        return redirect(url_for('admin_quests'))
    
    return render_template('admin/quest_edit.html', quest=quest, admin_secret=ADMIN_SECRET)


@app.route(f'/{ADMIN_SECRET}/quests/toggle/<int:quest_id>', methods=['POST'])
@admin_required
def admin_quest_toggle(quest_id):
    conn = get_db()
    quest = conn.execute('SELECT is_active FROM quest_templates WHERE id = ?', (quest_id,)).fetchone()
    if quest:
        new_status = 0 if quest['is_active'] else 1
        conn.execute('UPDATE quest_templates SET is_active = ? WHERE id = ?', (new_status, quest_id))
        conn.commit()
    conn.close()
    return redirect(url_for('admin_quests'))


@app.route(f'/{ADMIN_SECRET}/quests/delete/<int:quest_id>', methods=['POST'])
@admin_required
def admin_quest_delete(quest_id):
    conn = get_db()
    conn.execute('DELETE FROM quest_templates WHERE id = ?', (quest_id,))
    conn.commit()
    conn.close()
    flash('🗑️ Задание удалено', 'success')
    return redirect(url_for('admin_quests'))


@app.route(f'/{ADMIN_SECRET}/social_quests')
@admin_required
def admin_social_quests():
    conn = get_db()
    pending = conn.execute('''
        SELECT usq.*, u.login, qt.name 
        FROM user_social_quests usq
        JOIN users u ON usq.user_id = u.id
        JOIN quest_templates qt ON usq.quest_key = qt.quest_key
        WHERE usq.status = 'pending'
        ORDER BY usq.completed_at DESC
    ''').fetchall()
    conn.close()
    
    return render_template('admin/social_quests.html', 
                          pending=pending, 
                          admin_secret=ADMIN_SECRET)


@app.route(f'/{ADMIN_SECRET}/social_quests/approve/<int:user_id>/<quest_key>', methods=['POST'])
@admin_required
def admin_social_approve(user_id, quest_key):
    conn = get_db()
    
    # Обновляем статус
    conn.execute('''
        UPDATE user_social_quests 
        SET status = 'completed', completed_at = ?, claimed = 0 
        WHERE user_id = ? AND quest_key = ?
    ''', (time.time(), user_id, quest_key))
    
    # Начисляем награду
    template = conn.execute('SELECT reward FROM quest_templates WHERE quest_key = ?', (quest_key,)).fetchone()
    if template:
        conn.execute('UPDATE users SET bonus_balance = bonus_balance + ? WHERE id = ?', 
                    (template['reward'], user_id))
        conn.execute('UPDATE user_social_quests SET claimed = 1 WHERE user_id = ? AND quest_key = ?',
                    (user_id, quest_key))
    
    conn.commit()
    conn.close()
    
    flash('✅ Задание подтверждено, награда начислена', 'success')
    return redirect(url_for('admin_social_quests'))


@app.route(f'/{ADMIN_SECRET}/social_quests/reject/<int:user_id>/<quest_key>', methods=['POST'])
@admin_required
def admin_social_reject(user_id, quest_key):
    conn = get_db()
    conn.execute('DELETE FROM user_social_quests WHERE user_id = ? AND quest_key = ?', (user_id, quest_key))
    conn.commit()
    conn.close()
    
    flash('❌ Заявка отклонена', 'warning')
    return redirect(url_for('admin_social_quests'))

@app.errorhandler(500)
def internal_error(error):
    try:
        db = get_db()
        db.rollback()
    except:
        pass
    return render_template('500.html'), 500

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404



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
