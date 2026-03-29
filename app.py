"""
BizPilot AI v2.1 — Business Operating System
Run: streamlit run app.py
Deps: pip install streamlit requests pandas plotly apscheduler
"""

# ── CRITICAL: st.set_page_config must be the VERY FIRST streamlit call ──────
import streamlit as st

st.set_page_config(
    page_title="BizPilot AI",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── All other imports AFTER set_page_config ──────────────────────────────────
import json
import os
import time
import random
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
    PANDAS_OK = True
except Exception:
    PANDAS_OK = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

try:
    import requests
    REQUESTS_OK = True
except Exception:
    REQUESTS_OK = False

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    SCHEDULER_OK = True
except Exception:
    SCHEDULER_OK = False

# ═══════════════════════════════════════════════════════════════════════════
# DATA PATHS  — wrapped so a permission error never crashes the page
# ═══════════════════════════════════════════════════════════════════════════
try:
    DATA_DIR = Path("bizpilot_data")
    DATA_DIR.mkdir(exist_ok=True)
except Exception as _e:
    st.error(f"⚠️ Cannot create data directory: {_e}")
    st.stop()

OPS_FILE       = DATA_DIR / "operations.json"
CRM_FILE       = DATA_DIR / "crm.json"
CONFIG_FILE    = DATA_DIR / "config.json"
MESSAGES_FILE  = DATA_DIR / "messages.json"
USERS_FILE     = DATA_DIR / "users.json"
CONTACTS_FILE  = DATA_DIR / "contacts.json"
TEMPLATES_FILE = DATA_DIR / "templates.json"
RESPONSES_FILE = DATA_DIR / "responses.json"
ACTIVITY_FILE  = DATA_DIR / "activity.json"
SCHEDULE_FILE  = DATA_DIR / "schedule.json"
TASKS_FILE     = DATA_DIR / "tasks.json"

# ═══════════════════════════════════════════════════════════════════════════
# DEFAULTS
# ═══════════════════════════════════════════════════════════════════════════
DEFAULT_CONFIG = {
    "business_name": "My Business",
    "business_type": "Manufacturing / Distribution",
    "products": ["Wheat Flour (Atta)", "Semolina (Suji)", "Maida"],
    "currency": "₹",
    "language": "English",
    "owner_name": "Owner",
    "city": "Indore",
    "api_key": "",
    "ai_model": "openai/gpt-4o-mini",
    "target_daily_sales": 50000,
    "target_daily_production": 200,
}
DEFAULT_SCHEDULE = {
    "morning_time": "09:00", "evening_time": "19:00",
    "morning_enabled": True, "evening_enabled": True,
    "last_morning_run": None, "last_evening_run": None,
}

# ═══════════════════════════════════════════════════════════════════════════
# TRANSLATIONS
# ═══════════════════════════════════════════════════════════════════════════
T = {
    "English": {
        "app_name": "BizPilot AI", "tagline": "Business Operating System",
        "dashboard": "Dashboard", "daily_entry": "Daily Entry",
        "crm": "CRM", "reports": "Reports", "ai_assistant": "AI Assistant",
        "settings": "Settings", "workforce": "Workforce",
        "daily_ops": "Daily Operations", "owner_hq": "Owner HQ",
        "scheduler": "Scheduler", "activity": "Activity Log",
        "today_production": "Today's Production", "today_sales": "Today's Sales",
        "active_customers": "Active Customers", "pending_orders": "Pending Orders",
        "login": "Login", "logout": "Logout",
        "no_data": "No data yet. Start by adding daily entries!",
        "ai_thinking": "BizPilot AI is thinking...",
        "entry_saved": "✅ Entry saved successfully!",
    },
    "Hindi": {
        "app_name": "BizPilot AI", "tagline": "बिज़नेस ऑपरेटिंग सिस्टम",
        "dashboard": "डैशबोर्ड", "daily_entry": "दैनिक प्रविष्टि",
        "crm": "ग्राहक प्रबंधन", "reports": "रिपोर्ट",
        "ai_assistant": "AI सहायक", "settings": "सेटिंग्स",
        "workforce": "कार्यबल", "daily_ops": "दैनिक संचालन",
        "owner_hq": "मालिक पैनल", "scheduler": "शेड्यूलर",
        "activity": "गतिविधि लॉग",
        "today_production": "आज का उत्पादन", "today_sales": "आज की बिक्री",
        "active_customers": "सक्रिय ग्राहक", "pending_orders": "लंबित ऑर्डर",
        "login": "लॉगिन", "logout": "लॉगआउट",
        "no_data": "अभी तक कोई डेटा नहीं।",
        "ai_thinking": "BizPilot AI सोच रहा है...",
        "entry_saved": "✅ प्रविष्टि सहेजी गई!",
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# DATA HELPERS — every function safe, never raises
# ═══════════════════════════════════════════════════════════════════════════
def load_json(path, default):
    try:
        p = Path(path)
        if p.exists() and p.stat().st_size > 0:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default() if callable(default) else default

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        st.warning(f"Save error: {e}")

def load_config():    return load_json(CONFIG_FILE,    DEFAULT_CONFIG.copy())
def save_config(d):   save_json(CONFIG_FILE, d)
def load_ops():       return load_json(OPS_FILE,       [])
def save_ops(d):      save_json(OPS_FILE, d)
def load_crm():       return load_json(CRM_FILE,       [])
def save_crm(d):      save_json(CRM_FILE, d)
def load_users():     return load_json(USERS_FILE,     [])
def save_users(d):    save_json(USERS_FILE, d)
def load_contacts():  return load_json(CONTACTS_FILE,  [])
def save_contacts(d): save_json(CONTACTS_FILE, d)
def load_templates(): return load_json(TEMPLATES_FILE, {})
def save_templates(d):save_json(TEMPLATES_FILE, d)
def load_responses(): return load_json(RESPONSES_FILE, [])
def save_responses(d):save_json(RESPONSES_FILE, d)
def load_activity():  return load_json(ACTIVITY_FILE,  [])
def save_activity(d): save_json(ACTIVITY_FILE, d)
def load_schedule():  return load_json(SCHEDULE_FILE,  DEFAULT_SCHEDULE.copy())
def save_schedule(d): save_json(SCHEDULE_FILE, d)
def load_tasks():     return load_json(TASKS_FILE,     [])
def save_tasks(d):    save_json(TASKS_FILE, d)
def load_messages():  return load_json(MESSAGES_FILE,  [])
def save_messages(d): save_json(MESSAGES_FILE, d)

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def log_activity(action: str, user: str = "system", detail: str = ""):
    try:
        acts = load_activity()
        acts.append({"id": f"act_{int(time.time()*1000)}",
                     "timestamp": datetime.now().isoformat(),
                     "user": user, "action": action, "detail": detail})
        save_activity(acts[-500:])
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════
# SEED DEMO DATA
# ═══════════════════════════════════════════════════════════════════════════
def seed_all():
    try:
        if not load_users():
            save_users([
                {"id": "u1", "name": "Rajan Sharma",  "phone": "9826000001", "role": "Owner",
                 "department": "Management", "password": hash_pw("owner123"),   "active": True, "preferred_language": "English"},
                {"id": "u2", "name": "Vikram Singh",  "phone": "9826000002", "role": "Manager",
                 "department": "Operations", "password": hash_pw("manager123"), "active": True, "preferred_language": "Hindi"},
                {"id": "u3", "name": "Amit Patel",    "phone": "9826000003", "role": "Employee",
                 "department": "Sales",      "password": hash_pw("emp123"),     "active": True, "preferred_language": "Hindi"},
                {"id": "u4", "name": "Sunita Devi",   "phone": "9826000004", "role": "Employee",
                 "department": "Storage",    "password": hash_pw("emp123"),     "active": True, "preferred_language": "Hindi"},
                {"id": "u5", "name": "Rahul Joshi",   "phone": "9826000005", "role": "Employee",
                 "department": "Purchase",   "password": hash_pw("emp123"),     "active": True, "preferred_language": "Hindi"},
            ])

        if not load_crm():
            save_crm([
                {"id": "crm1", "name": "Ramesh Agarwal",  "phone": "9800011111", "requirement": "50 bags Atta weekly",
                 "status": "Converted",  "segment": "Bulk",        "last_contact": "2024-03-14",
                 "follow_up_date": "2024-03-21", "notes": "Regular since 2022", "created": "2024-01-10", "comm_log": []},
                {"id": "crm2", "name": "Sunita Bakery",   "phone": "9977001122", "requirement": "Maida 200kg/month",
                 "status": "Follow-up",  "segment": "Retail",      "last_contact": "2024-03-05",
                 "follow_up_date": "2024-03-19", "notes": "Interested in bulk discount", "created": "2024-03-05", "comm_log": []},
                {"id": "crm3", "name": "Sharma Traders",  "phone": "9012345678", "requirement": "Mixed 100 bags",
                 "status": "Interested", "segment": "Bulk",        "last_contact": "2024-03-18",
                 "follow_up_date": "2024-03-20", "notes": "First WhatsApp inquiry", "created": "2024-03-18", "comm_log": []},
                {"id": "crm4", "name": "Priya Sweets",    "phone": "8888123456", "requirement": "Suji 50kg weekly",
                 "status": "Converted",  "segment": "Retail",      "last_contact": "2024-03-13",
                 "follow_up_date": "2024-03-20", "notes": "Festival bulk buyer", "created": "2024-02-20", "comm_log": []},
                {"id": "crm5", "name": "Mohan Kirana",    "phone": "9765432100", "requirement": "Atta 20 bags/week",
                 "status": "Lost",       "segment": "Retail",      "last_contact": "2024-02-10",
                 "follow_up_date": None,          "notes": "Went with competitor", "created": "2024-01-28", "comm_log": []},
            ])

        if not load_ops():
            today = datetime.now()
            products = ["Wheat Flour (Atta)", "Semolina (Suji)", "Maida"]
            ops = []
            for i in range(14):
                d = today - timedelta(days=13 - i)
                ops.append({
                    "id": f"op_{i}", "date": d.strftime("%Y-%m-%d"),
                    "production": random.randint(150, 280), "sales": random.randint(35000, 75000),
                    "orders_received": random.randint(8, 25), "orders_completed": random.randint(6, 20),
                    "expenses": random.randint(8000, 18000),
                    "inventory_note": random.choice(["Stock normal", "Low on Maida", "Atta stock good", "Restock Suji"]),
                    "customer_inquiries": random.randint(3, 12),
                    "notes": random.choice(["Smooth ops", "Delivery delay", "New bulk order", "Machine maintenance", ""]),
                    "product": random.choice(products),
                })
            save_ops(ops)

        if not load_templates():
            save_templates({
                "Sales": {
                    "morning":    "🌅 Good morning {name}!\n\nToday's Sales Targets:\n• Target: ₹{target} sales\n• Follow ups: {followups} pending\n• Report back by 7 PM 💪",
                    "morning_hi": "🌅 शुभ प्रभात {name}!\n\nआज के लक्ष्य:\n• बिक्री लक्ष्य: ₹{target}\n• फॉलो-अप: {followups} बाकी\n\nशाम 7 बजे रिपोर्ट करें 💪",
                    "evening":    "📊 Evening Sales Report\n\nPlease reply:\n• Today's sales (₹)\n• Orders received\n• Orders delivered\n• Customer issues\n• Tomorrow's priority",
                    "evening_hi": "📊 शाम की बिक्री रिपोर्ट\n\nकृपया बताएं:\n• आज की बिक्री (₹)\n• ऑर्डर मिले\n• ऑर्डर डिलीवर\n• कोई समस्या\n• कल की प्राथमिकता",
                },
                "Purchase": {
                    "morning":    "🌅 Good morning {name}!\n\nToday's Purchase Tasks:\n• Check mandi rates\n• Confirm supplier orders\n• Review stock needs\n\nSend rate update by 10 AM.",
                    "morning_hi": "🌅 शुभ प्रभात {name}!\n\nआज के कार्य:\n• मंडी भाव जांचें\n• सप्लायर ऑर्डर कन्फर्म\n• स्टॉक ज़रूरतें देखें",
                    "evening":    "📊 Evening Purchase Report\n\nPlease reply:\n• Mandi rate (₹/quintal)\n• Wheat purchased (qtl)\n• Amount paid (₹)\n• Supplier issues",
                    "evening_hi": "📊 शाम की खरीद रिपोर्ट\n\nकृपया बताएं:\n• मंडी भाव (₹/क्विंटल)\n• खरीदा गेहूँ (क्विंटल)\n• भुगतान (₹)\n• सप्लायर समस्या",
                },
                "Storage": {
                    "morning":    "🌅 Good morning {name}!\n\nMorning Stock Check:\n• Count all product stock\n• Check machinery\n• Report damage/issues\n\nSend by 9:30 AM.",
                    "morning_hi": "🌅 शुभ प्रभात {name}!\n\nसुबह स्टॉक जाँच:\n• स्टॉक गिनें\n• मशीनरी जाँचें\n\nसुबह 9:30 बजे भेजें।",
                    "evening":    "📊 Evening Inventory\n\nPlease reply:\n• Atta stock (bags)\n• Suji stock (bags)\n• Maida stock (bags)\n• Production today\n• Low stock alerts",
                    "evening_hi": "📊 शाम की इन्वेंटरी\n\nकृपया बताएं:\n• आटा स्टॉक (बोरे)\n• सूजी स्टॉक\n• मैदा स्टॉक\n• आज का उत्पादन",
                },
                "Operations": {
                    "morning":    "🌅 Good morning {name}!\n\nOps Checklist:\n• Machine startup check\n• Staff attendance\n• Today's production plan\n\nConfirm by 9 AM.",
                    "morning_hi": "🌅 शुभ प्रभात {name}!\n\nऑपरेशन चेकलिस्ट:\n• मशीन जाँच\n• स्टाफ उपस्थिति\n• प्रोडक्शन योजना",
                    "evening":    "📊 Ops Summary\n\nPlease reply:\n• Production output (units)\n• Machine downtime (hrs)\n• Staff issues\n• Maintenance needed\n• Rating (1-10)",
                    "evening_hi": "📊 ऑपरेशन सारांश\n\nकृपया बताएं:\n• उत्पादन आउटपुट\n• मशीन डाउनटाइम\n• स्टाफ समस्याएं\n• रेटिंग (1-10)",
                },
            })

        if not load_contacts():
            save_contacts([
                {"id": "c1", "name": "Ramesh Agarwal",    "phone": "9800011111", "role": "Customer",    "type": "customer",     "segment": "Bulk",        "assigned_manager": "Vikram Singh", "preferred_language": "Hindi",   "active_status": True, "last_contact": "2024-03-10", "notes": "Regular bulk buyer"},
                {"id": "c2", "name": "Sharma Flour Depot","phone": "9800022222", "role": "Distributor", "type": "distributor",  "segment": "Distributor", "assigned_manager": "Vikram Singh", "preferred_language": "Hindi",   "active_status": True, "last_contact": "2024-03-12", "notes": "North zone"},
                {"id": "c3", "name": "Grain Masters Ltd", "phone": "9800033333", "role": "Supplier",    "type": "supplier",     "segment": None,          "assigned_manager": "Rahul Joshi",  "preferred_language": "English", "active_status": True, "last_contact": "2024-03-08", "notes": "Primary wheat supplier"},
            ])

        today_str = datetime.now().strftime("%Y-%m-%d")
        tasks = load_tasks()
        today_tasks = [t for t in tasks if t.get("date") == today_str]
        if not today_tasks:
            users = load_users()
            new_tasks = []
            for u in users:
                if u.get("role") == "Employee":
                    new_tasks.append({
                        "id": f"task_{u['id']}_{today_str}",
                        "user_id": u["id"], "user_name": u["name"],
                        "department": u.get("department", "Operations"),
                        "date": today_str,
                        "morning_sent": False, "morning_response": None, "morning_response_time": None,
                        "evening_sent": False, "evening_response": None, "evening_response_time": None,
                        "status": "Pending", "parsed_data": None,
                    })
            save_tasks(tasks + new_tasks)
    except Exception as e:
        # Seed failure must never crash the app
        pass

# ═══════════════════════════════════════════════════════════════════════════
# AI ENGINE
# ═══════════════════════════════════════════════════════════════════════════
def call_ai(prompt: str, system: str, api_key: str, language: str = "English") -> str:
    try:
        if not api_key or not api_key.strip() or not REQUESTS_OK:
            return _mock_ai(prompt, language)
        lang_hint = "Respond in Hindi (Devanagari script)." if language == "Hindi" else "Respond in English."
        cfg = load_config()
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key.strip()}"}
        body = {
            "model": cfg.get("ai_model", "openai/gpt-4o-mini"),
            "messages": [
                {"role": "system", "content": f"{system}\n\n{lang_hint}"},
                {"role": "user",   "content": prompt},
            ],
            "max_tokens": 900,
        }
        r = requests.post("https://openrouter.ai/api/v1/chat/completions",
                          headers=headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        return f"⚠️ AI Error: {e}\n\n{_mock_ai(prompt, language)}"


def parse_response_to_data(raw_text: str, department: str, api_key: str) -> dict:
    try:
        if not api_key or not api_key.strip() or not REQUESTS_OK:
            return _mock_parse(raw_text, department)
        system = "Extract key business metrics from informal messages. Return ONLY valid JSON. No explanation."
        dept_fields = {
            "Sales":      '{"sales_amount":0,"orders_received":0,"orders_delivered":0,"issues":"","tomorrow_plan":""}',
            "Purchase":   '{"mandi_rate":0,"wheat_purchased_quintal":0,"amount_paid":0,"supplier_issues":"","tomorrow_plan":""}',
            "Storage":    '{"atta_stock":0,"suji_stock":0,"maida_stock":0,"production_units":0,"low_stock_alerts":""}',
            "Operations": '{"production_output":0,"downtime_hours":0,"staff_issues":"","maintenance_needed":"","rating":0}',
        }
        schema = dept_fields.get(department, '{"value":0,"notes":""}')
        prompt = f"Extract data matching schema: {schema}\n\nMessage: {raw_text}"
        cfg = load_config()
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key.strip()}"}
        body = {"model": cfg.get("ai_model", "openai/gpt-4o-mini"),
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                "max_tokens": 300}
        r = requests.post("https://openrouter.ai/api/v1/chat/completions",
                          headers=headers, json=body, timeout=20)
        text = r.json()["choices"][0]["message"]["content"]
        s, e = text.find("{"), text.rfind("}") + 1
        return json.loads(text[s:e]) if s != -1 else {"raw": raw_text}
    except Exception:
        return _mock_parse(raw_text, department)


def _mock_parse(text: str, department: str) -> dict:
    nums = [int(n.replace(",", "")) for n in re.findall(r'\d[\d,]*', text) if n]
    if department == "Sales":
        return {"sales_amount": nums[0] if nums else 0, "orders_received": nums[1] if len(nums) > 1 else 0,
                "orders_delivered": nums[2] if len(nums) > 2 else 0, "issues": "", "tomorrow_plan": "Follow-ups"}
    if department == "Purchase":
        return {"mandi_rate": nums[0] if nums else 0, "wheat_purchased_quintal": nums[1] if len(nums) > 1 else 0,
                "amount_paid": nums[2] if len(nums) > 2 else 0, "supplier_issues": "", "tomorrow_plan": ""}
    if department == "Storage":
        return {"atta_stock": nums[0] if nums else 0, "suji_stock": nums[1] if len(nums) > 1 else 0,
                "maida_stock": nums[2] if len(nums) > 2 else 0, "production_units": nums[3] if len(nums) > 3 else 0,
                "low_stock_alerts": ""}
    return {"raw": text}


def _mock_ai(prompt: str, language: str) -> str:
    p = prompt.lower()
    ops  = load_ops()
    crm  = load_crm()
    cfg  = load_config()
    last7 = ops[-7:] if len(ops) >= 7 else ops
    n = max(len(last7), 1)
    ts = sum(o.get("sales", 0) for o in last7)
    tp = sum(o.get("production", 0) for o in last7)
    te = sum(o.get("expenses", 0) for o in last7)
    if language == "Hindi":
        if "बिक्री" in p or "sales" in p:
            return f"💰 **पिछले {n} दिनों की बिक्री:** ₹{ts:,}\n📈 दैनिक औसत: ₹{ts//n:,}\n\n💡 बिक्री लक्ष्य से {'ऊपर ✅' if ts/n > cfg.get('target_daily_sales',50000) else 'नीचे ⚠️'} है।"
        if "उत्पादन" in p or "production" in p:
            return f"🏭 **औसत उत्पादन:** {tp//n} इकाइयाँ/दिन\n💡 लक्ष्य {cfg.get('target_daily_production',200)} इकाइयाँ/दिन है।"
        return f"🤖 **BizPilot AI:**\n- कुल बिक्री: ₹{ts:,}\n- औसत उत्पादन: {tp//n} इकाइयाँ\n- CRM: {len(crm)} लीड\n\n💡 नियमित फॉलो-अप से रूपांतरण दर बढ़ेगी।"
    else:
        conv = sum(1 for c in crm if c["status"] == "Converted")
        foll = sum(1 for c in crm if c["status"] == "Follow-up")
        if "report" in p or "summary" in p:
            return f"""📋 **Business Summary — Last {n} Days**

**Sales:** ₹{ts:,} total | ₹{ts//n:,}/day avg
**Production:** {tp} units | {tp//n}/day avg
**Expenses:** ₹{te:,} | Estimated profit: ₹{ts-te:,}
**Orders:** {sum(o.get('orders_received',0) for o in last7)} received | {sum(o.get('orders_completed',0) for o in last7)} completed
**CRM:** {len(crm)} leads | {conv} converted | {foll} need follow-up

**Recommendations:**
1. Contact {foll} pending follow-up leads this week
2. Sales avg ₹{ts//n:,}/day vs target ₹{cfg.get('target_daily_sales',50000):,}
3. Review inventory notes for restock alerts"""
        if "crm" in p or "customer" in p:
            return f"👥 **CRM:** {len(crm)} leads | {conv} converted ({int(conv/max(len(crm),1)*100)}%) | {foll} follow-up needed\n\n💡 Contact follow-up leads today — they've shown interest!"
        if "employee" in p or "staff" in p or "task" in p:
            tasks = load_tasks()
            today = datetime.now().strftime("%Y-%m-%d")
            tt = [t for t in tasks if t.get("date") == today]
            sub = sum(1 for t in tt if t.get("evening_response"))
            return f"👥 **Employee Status:** {sub}/{len(tt)} reports submitted today\n\n💡 Send reminders to the {len(tt)-sub} employees who haven't reported."
        return f"🤖 **BizPilot AI Snapshot:**\n- 7-day Sales: ₹{ts:,} | Avg: ₹{ts//n:,}/day\n- Avg Production: {tp//n} units/day\n- CRM Conversion: {int(conv/max(len(crm),1)*100)}%\n- Expenses: ₹{te:,}\n\n💡 Use quick prompts above for specific insights!"


def build_system_prompt(cfg: dict) -> str:
    return (f"You are BizPilot AI — a smart operations assistant for {cfg.get('business_name','My Business')}, "
            f"a {cfg.get('business_type','manufacturing')} business in {cfg.get('city','India')}. "
            f"Products: {', '.join(cfg.get('products',[]))}. Currency: {cfg.get('currency','₹')}. "
            "Be practical, concise, use bullets and emojis. Understand Hindi/English mixed messages.")

# ═══════════════════════════════════════════════════════════════════════════
# MESSAGING ENGINE
# ═══════════════════════════════════════════════════════════════════════════
def send_message(contact_name: str, phone: str, message: str,
                 channel: str = "WhatsApp", msg_type: str = "general",
                 sender: str = "system") -> dict:
    record = {"id": f"msg_{int(time.time()*1000)}", "timestamp": datetime.now().isoformat(),
              "recipient": contact_name, "phone": phone, "channel": channel,
              "message": message, "type": msg_type, "status": "Simulated ✅", "sent_by": sender}
    try:
        msgs = load_messages()
        msgs.append(record)
        save_messages(msgs[-1000:])
        log_activity(f"Message sent to {contact_name}", sender, f"{channel}:{msg_type}")
    except Exception:
        pass
    return record


def run_daily_automation(msg_type: str = "morning", triggered_by: str = "system"):
    users     = load_users()
    templates = load_templates()
    cfg       = load_config()
    today     = datetime.now().strftime("%Y-%m-%d")
    tasks     = load_tasks()
    results   = []
    for user in users:
        if not user.get("active", True):
            continue
        dept = user.get("department", "Operations")
        lang = user.get("preferred_language", "English")
        tmpl_dept = templates.get(dept, templates.get("Operations", {}))
        if msg_type == "morning":
            key = "morning_hi" if lang == "Hindi" else "morning"
            tmpl = tmpl_dept.get(key, tmpl_dept.get("morning", "Good morning {name}!"))
            msg = tmpl.format(name=user["name"].split()[0],
                              target=cfg.get("target_daily_sales", 50000),
                              followups=random.randint(2, 5), visits=random.randint(1, 3))
            task = next((t for t in tasks if t.get("user_id") == user["id"] and t.get("date") == today), None)
            if not task:
                task = {"id": f"task_{user['id']}_{today}", "user_id": user["id"],
                        "user_name": user["name"], "department": dept, "date": today,
                        "morning_sent": False, "morning_response": None, "morning_response_time": None,
                        "evening_sent": False, "evening_response": None, "evening_response_time": None,
                        "status": "Pending", "parsed_data": None}
                tasks.append(task)
            task["morning_sent"] = True
        else:
            key = "evening_hi" if lang == "Hindi" else "evening"
            tmpl = tmpl_dept.get(key, tmpl_dept.get("evening", "Please submit your report."))
            msg = tmpl.format(name=user["name"].split()[0])
            task = next((t for t in tasks if t.get("user_id") == user["id"] and t.get("date") == today), None)
            if task:
                task["evening_sent"] = True
        rec = send_message(user["name"], user.get("phone", ""), msg, "WhatsApp", msg_type, triggered_by)
        results.append(rec)
    save_tasks(tasks)
    log_activity(f"Automation:{msg_type}", triggered_by, f"{len(results)} sent")
    return results, len(results)


def simulate_employee_response(task: dict, raw_response: str, cfg: dict):
    try:
        tasks = load_tasks()
        for t in tasks:
            if t["id"] == task["id"]:
                t["evening_response"] = raw_response
                t["evening_response_time"] = datetime.now().isoformat()
                t["status"] = "Submitted"
                t["parsed_data"] = parse_response_to_data(raw_response, t.get("department", "Operations"),
                                                          cfg.get("api_key", ""))
                responses = load_responses()
                responses.append({
                    "id": f"resp_{int(time.time()*1000)}", "user_id": t["user_id"],
                    "user_name": t["user_name"], "department": t["department"],
                    "type": "evening", "date": t["date"], "raw_message": raw_response,
                    "parsed_data": t["parsed_data"], "timestamp": datetime.now().isoformat(),
                })
                save_responses(responses)
                break
        save_tasks(tasks)
        log_activity(f"Response from {task['user_name']}", task["user_name"], "Evening submitted")
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════
_sched = None

def apply_schedule(sched_cfg: dict) -> bool:
    global _sched
    if not SCHEDULER_OK:
        return False
    try:
        if _sched is None or not _sched.running:
            _sched = BackgroundScheduler()
            _sched.start()
        for job in _sched.get_jobs():
            job.remove()
        if sched_cfg.get("morning_enabled"):
            mh, mm = sched_cfg.get("morning_time", "09:00").split(":")
            _sched.add_job(lambda: run_daily_automation("morning", "scheduler"),
                           CronTrigger(hour=int(mh), minute=int(mm)), id="morning_job")
        if sched_cfg.get("evening_enabled"):
            eh, em = sched_cfg.get("evening_time", "19:00").split(":")
            _sched.add_job(lambda: run_daily_automation("evening", "scheduler"),
                           CronTrigger(hour=int(eh), minute=int(em)), id="evening_job")
        return True
    except Exception:
        return False

# ═══════════════════════════════════════════════════════════════════════════
# CSS  — safe injection, dark theme
# IMPORTANT: do NOT set display:none on .main or .block-container
# ═══════════════════════════════════════════════════════════════════════════
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500;600&display=swap');
:root{
  --bg:#0D1117;--bg-card:#161B22;--bg-card2:#1C2333;
  --acc:#F97316;--acc2:#FBBF24;
  --green:#10B981;--blue:#3B82F6;--red:#EF4444;--purple:#8B5CF6;
  --txt:#F0F6FC;--txt2:#8B949E;--border:#30363D;
  --b-acc:rgba(249,115,22,.4);--glow:0 0 20px rgba(249,115,22,.15);
}
/* Dark base */
html,body{background-color:#0D1117 !important;}
.stApp{background-color:#0D1117 !important;}
/* Typography */
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:#F0F6FC;}
/* Main content area */
.main .block-container{padding:1.5rem 2rem 2rem;max-width:1440px;}
/* Sidebar */
[data-testid="stSidebar"]{background:#161B22 !important;border-right:1px solid #30363D !important;}
/* Hide Streamlit chrome only — NOT content containers */
#MainMenu{visibility:hidden;}
footer{visibility:hidden;}
header{visibility:hidden;}
[data-testid="stDecoration"]{display:none;}
/* Metric cards */
.mc{background:#161B22;border:1px solid #30363D;border-radius:16px;
    padding:1.2rem 1.4rem;transition:all .25s;position:relative;overflow:hidden;}
.mc::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:16px 16px 0 0;}
.mc.or::before{background:linear-gradient(90deg,#F97316,#FBBF24);}
.mc.gr::before{background:linear-gradient(90deg,#10B981,#34D399);}
.mc.bl::before{background:linear-gradient(90deg,#3B82F6,#60A5FA);}
.mc.pu::before{background:linear-gradient(90deg,#8B5CF6,#A78BFA);}
.mc.re::before{background:linear-gradient(90deg,#EF4444,#F87171);}
.mc:hover{border-color:rgba(249,115,22,.4);transform:translateY(-2px);box-shadow:0 0 20px rgba(249,115,22,.15);}
.mc-icon{font-size:1.7rem;margin-bottom:.35rem;}
.mc-val{font-family:'Syne',sans-serif;font-size:1.8rem;font-weight:800;line-height:1;margin-bottom:.2rem;color:#F0F6FC;}
.mc-lbl{font-size:.76rem;color:#8B949E;text-transform:uppercase;letter-spacing:.08em;font-weight:500;}
.mc-delta{font-size:.76rem;margin-top:.3rem;}
.mc-delta.up{color:#10B981;}.mc-delta.dn{color:#EF4444;}
/* Page header */
.ph{background:linear-gradient(135deg,rgba(249,115,22,.08),rgba(251,191,36,.04));
    border:1px solid rgba(249,115,22,.35);border-radius:18px;padding:1.5rem 1.8rem;margin-bottom:1.5rem;
    display:flex;align-items:center;justify-content:space-between;}
.ph-title{font-family:'Syne',sans-serif;font-size:1.9rem;font-weight:800;
    background:linear-gradient(135deg,#F97316,#FBBF24);-webkit-background-clip:text;-webkit-text-fill-color:transparent;line-height:1.1;}
.ph-sub{color:#8B949E;font-size:.86rem;margin-top:.25rem;}
.ph-badge{background:rgba(249,115,22,.12);border:1px solid rgba(249,115,22,.28);
    color:#F97316;padding:.22rem .75rem;border-radius:99px;font-size:.73rem;font-weight:600;}
/* Logo */
.logo-wrap{display:flex;align-items:center;gap:.7rem;padding:.9rem .5rem 1.3rem;
    border-bottom:1px solid #30363D;margin-bottom:.9rem;}
.logo-icon{width:38px;height:38px;background:linear-gradient(135deg,#F97316,#FBBF24);
    border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:1.1rem;}
.logo-txt{font-family:'Syne',sans-serif;font-size:1.2rem;font-weight:800;
    background:linear-gradient(135deg,#F97316,#FBBF24);-webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.logo-ver{font-size:.6rem;color:#8B949E;margin-top:-2px;}
/* User badge */
.user-badge{background:rgba(249,115,22,.07);border:1px solid rgba(249,115,22,.18);
    border-radius:10px;padding:.6rem .85rem;margin-bottom:.9rem;font-size:.82rem;}
.role-badge{display:inline-block;padding:.12rem .55rem;border-radius:99px;font-size:.68rem;font-weight:600;}
.rb-owner{background:rgba(251,191,36,.15);color:#FBBF24;}
.rb-manager{background:rgba(59,130,246,.15);color:#60A5FA;}
.rb-employee{background:rgba(16,185,129,.15);color:#10B981;}
/* Status badges */
.badge{display:inline-block;padding:.16rem .65rem;border-radius:99px;font-size:.71rem;font-weight:600;}
.b-conv{background:rgba(16,185,129,.15);color:#10B981;border:1px solid rgba(16,185,129,.3);}
.b-foll{background:rgba(251,191,36,.15);color:#FBBF24;border:1px solid rgba(251,191,36,.3);}
.b-int{background:rgba(59,130,246,.15);color:#60A5FA;border:1px solid rgba(59,130,246,.3);}
.b-lost{background:rgba(239,68,68,.15);color:#EF4444;border:1px solid rgba(239,68,68,.3);}
.b-ok{background:rgba(16,185,129,.15);color:#10B981;border:1px solid rgba(16,185,129,.3);}
.b-warn{background:rgba(251,191,36,.15);color:#FBBF24;border:1px solid rgba(251,191,36,.3);}
.b-err{background:rgba(239,68,68,.15);color:#EF4444;border:1px solid rgba(239,68,68,.3);}
.b-info{background:rgba(59,130,246,.15);color:#60A5FA;border:1px solid rgba(59,130,246,.3);}
.b-pu{background:rgba(139,92,246,.15);color:#A78BFA;border:1px solid rgba(139,92,246,.3);}
/* Chat bubbles */
.chat-user{background:rgba(59,130,246,.09);border:1px solid rgba(59,130,246,.22);
    border-radius:14px 14px 4px 14px;padding:.85rem 1.1rem;margin:.45rem 0;margin-left:2.5rem;color:#F0F6FC;}
.chat-ai{background:rgba(249,115,22,.06);border:1px solid rgba(249,115,22,.16);
    border-radius:14px 14px 14px 4px;padding:.85rem 1.1rem;margin:.45rem 0;margin-right:2.5rem;line-height:1.65;color:#F0F6FC;}
.chat-hdr{font-size:.71rem;color:#F97316;font-weight:600;letter-spacing:.05em;margin-bottom:.4rem;}
/* Section header */
.sh{font-family:'Syne',sans-serif;font-size:1.2rem;font-weight:700;color:#F0F6FC;
    margin-bottom:.85rem;padding-bottom:.45rem;border-bottom:2px solid #30363D;}
/* Card box */
.cbox{background:#161B22;border:1px solid #30363D;border-radius:13px;padding:1.2rem;margin-bottom:.85rem;}
/* Report box */
.rbox{background:linear-gradient(135deg,rgba(249,115,22,.04),rgba(139,92,246,.04));
    border:1px solid rgba(249,115,22,.16);border-radius:13px;padding:1.3rem;line-height:1.8;font-size:.92rem;color:#F0F6FC;}
/* Message preview */
.mbox{background:#0d1f0d;border:1px solid rgba(16,185,129,.28);border-radius:9px;
    padding:.85rem 1rem;font-family:monospace;font-size:.83rem;color:#86EFAC;white-space:pre-wrap;}
/* Data table */
.dtbl{width:100%;border-collapse:collapse;}
.dtbl th{font-size:.7rem;text-transform:uppercase;letter-spacing:.08em;color:#8B949E;
    padding:.6rem .9rem;border-bottom:1px solid #30363D;text-align:left;}
.dtbl td{padding:.65rem .9rem;border-bottom:1px solid rgba(48,54,61,.45);font-size:.86rem;color:#F0F6FC;}
.dtbl tr:hover td{background:rgba(249,115,22,.03);}
/* Login */
.login-outer{display:flex;justify-content:center;align-items:flex-start;padding-top:3rem;}
/* Streamlit widget overrides */
.stButton>button{
    background:linear-gradient(135deg,#F97316,#EA580C) !important;
    color:white !important;border:none !important;border-radius:9px !important;
    font-family:'DM Sans',sans-serif !important;font-weight:600 !important;
    transition:all .18s !important;}
.stButton>button:hover{
    transform:translateY(-2px) !important;
    box-shadow:0 5px 18px rgba(249,115,22,.32) !important;}
.stTextInput>div>input,
.stTextArea>div>textarea,
.stSelectbox>div>div,
.stNumberInput>div>input,
.stDateInput>div>input{
    background:#1C2333 !important;border:1px solid #30363D !important;
    border-radius:9px !important;color:#F0F6FC !important;}
.stTextInput>div>input:focus,
.stTextArea>div>textarea:focus{
    border-color:rgba(249,115,22,.45) !important;
    box-shadow:0 0 0 2px rgba(249,115,22,.09) !important;}
label,.stMarkdown p{color:#8B949E !important;font-size:.85rem !important;}
.stSuccess{background:rgba(16,185,129,.09) !important;border:1px solid rgba(16,185,129,.28) !important;
    border-radius:9px !important;}
.stWarning{background:rgba(251,191,36,.09) !important;border:1px solid rgba(251,191,36,.28) !important;
    border-radius:9px !important;}
.stTabs [data-baseweb="tab-list"]{
    background:#161B22 !important;border-radius:11px !important;
    padding:.25rem !important;gap:.25rem !important;border:1px solid #30363D !important;}
.stTabs [data-baseweb="tab"]{border-radius:7px !important;color:#8B949E !important;font-weight:500 !important;}
.stTabs [aria-selected="true"]{
    background:linear-gradient(135deg,#F97316,#EA580C) !important;color:white !important;}
.stExpander{background:#161B22 !important;border:1px solid #30363D !important;border-radius:11px !important;}
hr{border-color:#30363D !important;}
div[data-testid="stHorizontalBlock"]{gap:.85rem;}
</style>
"""

def inject_css():
    try:
        st.markdown(CSS, unsafe_allow_html=True)
    except Exception:
        pass  # Never crash on CSS failure

# ═══════════════════════════════════════════════════════════════════════════
# SESSION STATE INIT  — safe, no st.rerun() inside
# ═══════════════════════════════════════════════════════════════════════════
def init_session():
    try:
        if "logged_in" not in st.session_state:
            st.session_state.logged_in = False
        if "current_user" not in st.session_state:
            st.session_state.current_user = None
        if "page" not in st.session_state:
            st.session_state.page = "Dashboard"
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        if "config" not in st.session_state:
            st.session_state.config = load_config()
        if "seeded" not in st.session_state:
            st.session_state.seeded = False
        # Seed once — no st.rerun() here
        if not st.session_state.seeded:
            seed_all()
            st.session_state.seeded = True
    except Exception as e:
        # If session init fails partially, still show the app
        if "logged_in" not in st.session_state:
            st.session_state.logged_in = False
        if "page" not in st.session_state:
            st.session_state.page = "Dashboard"
        if "config" not in st.session_state:
            st.session_state.config = DEFAULT_CONFIG.copy()

# ═══════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════
def page_login():
    cfg  = st.session_state.config
    lang = cfg.get("language", "English")

    inject_css()

    # Center column
    _, col, _ = st.columns([1, 1.1, 1])
    with col:
        st.markdown("""
        <div style="text-align:center;padding:2rem 0 1.5rem;">
          <div style="font-size:2.8rem;">🚀</div>
          <div style="font-family:'Syne',sans-serif;font-size:2rem;font-weight:800;
               background:linear-gradient(135deg,#F97316,#FBBF24);
               -webkit-background-clip:text;-webkit-text-fill-color:transparent;">BizPilot AI</div>
          <div style="color:#8B949E;font-size:.85rem;margin-top:.3rem;">Business Operating System v2.1</div>
        </div>""", unsafe_allow_html=True)

        with st.form("login_form"):
            st.markdown("### 🔐 Login")
            username = st.text_input("Username / Phone", placeholder="Rajan or 9826000001")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("🚀 Login", use_container_width=True)

        if submitted:
            try:
                users  = load_users()
                hashed = hash_pw(password)
                user   = next(
                    (u for u in users
                     if (u["name"].split()[0].lower() == username.lower()
                         or u["phone"] == username
                         or u["name"].lower() == username.lower())
                     and u["password"] == hashed
                     and u.get("active", True)),
                    None
                )
                if user:
                    st.session_state.logged_in = True
                    st.session_state.current_user = user
                    log_activity("Login", user["name"], f"Role:{user['role']}")
                    st.success(f"Welcome, {user['name']}! 👋")
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials.")
            except Exception as e:
                st.error(f"Login error: {e}")

        st.markdown("""
        <div style="background:#161B22;border:1px solid #30363D;border-radius:11px;
             padding:.9rem 1.1rem;margin-top:.8rem;font-size:.8rem;">
          <strong style="color:#F97316;">🎯 Demo Accounts:</strong><br>
          <span style="color:#8B949E;">
          👑 Owner &nbsp;&nbsp;→ <code>Rajan</code> / <code>owner123</code><br>
          🏢 Manager → <code>Vikram</code> / <code>manager123</code><br>
          👤 Employee → <code>Amit</code> / <code>emp123</code>
          </span>
        </div>""", unsafe_allow_html=True)


def check_access(roles: list) -> bool:
    u = st.session_state.get("current_user")
    return bool(u and u.get("role") in roles)

# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════
def render_sidebar():
    cfg  = st.session_state.config
    lang = cfg.get("language", "English")
    t    = T[lang]
    user = st.session_state.current_user or {}
    role = user.get("role", "Employee")

    with st.sidebar:
        st.markdown(f"""
        <div class="logo-wrap">
          <div class="logo-icon">🚀</div>
          <div>
            <div class="logo-txt">{t['app_name']}</div>
            <div class="logo-ver">{t['tagline']} • v2.1</div>
          </div>
        </div>""", unsafe_allow_html=True)

        role_cls = {"Owner": "rb-owner", "Manager": "rb-manager", "Employee": "rb-employee"}.get(role, "rb-employee")
        st.markdown(f"""
        <div class="user-badge">
          <div style="color:#F97316;font-weight:600;">{user.get('name','—')}</div>
          <div style="margin-top:.25rem;">
            <span class="role-badge {role_cls}">{role}</span>
            <span style="color:#8B949E;font-size:.72rem;margin-left:.35rem;">{user.get('department','')}</span>
          </div>
          <div style="color:#8B949E;font-size:.7rem;margin-top:.2rem;">📱 {user.get('phone','')}</div>
        </div>""", unsafe_allow_html=True)

        def nav(key, icon, label):
            active = st.session_state.page == key
            if st.button(f"{icon}  {label}", key=f"nav_{key}", use_container_width=True,
                         type="primary" if active else "secondary"):
                st.session_state.page = key
                st.rerun()

        nav("Dashboard",    "📊", t["dashboard"])
        nav("Daily Entry",  "📝", t["daily_entry"])
        nav("Daily Ops",    "🏭", t["daily_ops"])
        nav("CRM",          "👥", t["crm"])
        nav("Reports",      "📈", t["reports"])
        nav("AI Assistant", "🤖", t["ai_assistant"])

        if role in ["Owner", "Manager"]:
            nav("Workforce", "👤", t["workforce"])
            nav("Scheduler", "⏰", t["scheduler"])

        if role == "Owner":
            nav("Owner HQ", "👑", t["owner_hq"])

        nav("Activity", "📋", t["activity"])
        nav("Settings",  "⚙️", t["settings"])

        st.markdown("---")
        new_lang = st.selectbox(
            "🌐 Language", ["English", "Hindi"],
            index=0 if lang == "English" else 1,
            label_visibility="visible", key="lang_sel"
        )
        if new_lang != lang:
            st.session_state.config["language"] = new_lang
            save_config(st.session_state.config)
            st.rerun()

        if st.button("🚪 Logout", use_container_width=True):
            log_activity("Logout", user.get("name", "unknown"))
            st.session_state.logged_in    = False
            st.session_state.current_user = None
            st.session_state.chat_history = []
            st.rerun()

        st.markdown(f"""
        <div style="color:#8B949E;font-size:.7rem;text-align:center;margin-top:1rem;
             padding-top:.7rem;border-top:1px solid #30363D;">
          📅 {datetime.now().strftime('%d %B %Y')}<br>
          🕐 {datetime.now().strftime('%I:%M %p')}
        </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════
def page_dashboard():
    cfg  = st.session_state.config
    lang = cfg.get("language", "English")
    t    = T[lang]
    user = st.session_state.current_user or {}
    ops  = load_ops()
    crm  = load_crm()
    tasks = load_tasks()
    today     = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    today_ops  = next((o for o in reversed(ops) if o.get("date") == today),     None)
    yest_ops   = next((o for o in reversed(ops) if o.get("date") == yesterday), None)
    last7      = ops[-7:] if len(ops) >= 7 else ops
    today_tasks = [tk for tk in tasks if tk.get("date") == today]
    submitted   = sum(1 for tk in today_tasks if tk.get("evening_response"))

    hour = datetime.now().hour
    if lang == "Hindi":
        greeting = "शुभ प्रभात" if hour < 12 else ("शुभ दोपहर" if hour < 17 else "शुभ संध्या")
    else:
        greeting = "Good morning" if hour < 12 else ("Good afternoon" if hour < 17 else "Good evening")

    st.markdown(f"""
    <div class="ph">
      <div>
        <div class="ph-title">📊 {t['dashboard']}</div>
        <div class="ph-sub">{greeting}, {user.get('name','').split()[0]} 👋 — {datetime.now().strftime('%A, %d %B %Y')}</div>
      </div>
      <div class="ph-badge">🟢 LIVE</div>
    </div>""", unsafe_allow_html=True)

    def delta_html(cur, prev):
        if prev and prev != 0:
            p = ((cur - prev) / prev) * 100
            cls = "up" if p >= 0 else "dn"
            arr = "↑" if p >= 0 else "↓"
            return f'<div class="mc-delta {cls}">{arr} {abs(p):.1f}% vs yesterday</div>'
        return ""

    prod_v   = today_ops["production"] if today_ops else 0
    sales_v  = today_ops["sales"]      if today_ops else 0
    active_c = sum(1 for c in crm if c.get("status") in ["Interested", "Follow-up", "Converted"])
    pend_ord = max(sum(o.get("orders_received", 0) - o.get("orders_completed", 0) for o in last7[-3:]), 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f'<div class="mc or"><div class="mc-icon">🏭</div><div class="mc-val">{prod_v:,}</div><div class="mc-lbl">{t["today_production"]}</div>{delta_html(prod_v, yest_ops["production"] if yest_ops else None)}</div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="mc gr"><div class="mc-icon">💰</div><div class="mc-val">₹{sales_v:,}</div><div class="mc-lbl">{t["today_sales"]}</div>{delta_html(sales_v, yest_ops["sales"] if yest_ops else None)}</div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="mc bl"><div class="mc-icon">👥</div><div class="mc-val">{active_c}</div><div class="mc-lbl">{t["active_customers"]}</div></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="mc pu"><div class="mc-icon">📦</div><div class="mc-val">{pend_ord}</div><div class="mc-lbl">{t["pending_orders"]}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    e1, e2, e3, e4 = st.columns(4)
    foll = sum(1 for c in crm if c.get("status") == "Follow-up")
    conv = sum(1 for c in crm if c.get("status") == "Converted")
    conv_rate = int(conv / max(len(crm), 1) * 100)
    e1.markdown(f'<div class="mc gr"><div class="mc-val">{submitted}</div><div class="mc-lbl">Reports Today</div></div>', unsafe_allow_html=True)
    e2.markdown(f'<div class="mc re"><div class="mc-val">{len(today_tasks)-submitted}</div><div class="mc-lbl">Pending Reports</div></div>', unsafe_allow_html=True)
    e3.markdown(f'<div class="mc bl"><div class="mc-val">{foll}</div><div class="mc-lbl">Follow-up Leads</div></div>', unsafe_allow_html=True)
    e4.markdown(f'<div class="mc pu"><div class="mc-val">{conv_rate}%</div><div class="mc-lbl">CRM Conversion</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    cl, cr = st.columns([3, 2])

    with cl:
        st.markdown('<div class="sh">📈 Sales & Production — Last 7 Days</div>', unsafe_allow_html=True)
        if last7 and PLOTLY_OK:
            df = pd.DataFrame(last7) if PANDAS_OK else None
            if df is not None:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=df["date"], y=df["sales"], name="Sales ₹", marker_color="#F97316", opacity=.85))
                fig.add_trace(go.Scatter(x=df["date"], y=df["production"], name="Production", yaxis="y2",
                                          line=dict(color="#3B82F6", width=2.5), mode="lines+markers"))
                fig.update_layout(plot_bgcolor="#161B22", paper_bgcolor="#161B22", font_color="#8B949E",
                                  yaxis=dict(gridcolor="#30363D"), yaxis2=dict(overlaying="y", side="right"),
                                  xaxis=dict(gridcolor="#30363D"), margin=dict(l=0,r=0,t=10,b=0), height=280,
                                  legend=dict(orientation="h", y=-0.2, bgcolor="rgba(0,0,0,0)"))
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(t["no_data"])

    with cr:
        st.markdown('<div class="sh">🎯 CRM Funnel</div>', unsafe_allow_html=True)
        if crm and PLOTLY_OK:
            sc = {}
            for c in crm:
                sc[c.get("status", "?")] = sc.get(c.get("status", "?"), 0) + 1
            fig2 = go.Figure(go.Pie(labels=list(sc.keys()), values=list(sc.values()), hole=.55,
                                     marker_colors=["#10B981","#FBBF24","#3B82F6","#EF4444"]))
            fig2.update_layout(plot_bgcolor="#161B22", paper_bgcolor="#161B22", font_color="#8B949E",
                               margin=dict(l=0,r=0,t=10,b=0), height=280,
                               annotations=[dict(text=f"{len(crm)}<br>leads", x=.5, y=.5,
                                                 font_size=14, font_color="#F0F6FC", showarrow=False)])
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="sh">👥 Employee Report Status — Today</div>', unsafe_allow_html=True)
    if today_tasks:
        rows = ""
        for tk in today_tasks:
            ev    = tk.get("evening_response")
            ms    = tk.get("morning_sent", False)
            badge = "b-ok" if ev else ("b-warn" if tk.get("evening_sent") else "b-err")
            stat  = "✅ Submitted" if ev else ("⏳ Sent" if tk.get("evening_sent") else "❌ Not sent")
            rows += f"<tr><td><strong>{tk['user_name']}</strong></td><td>{tk.get('department','—')}</td><td>{'✅' if ms else '❌'} Morning</td><td><span class='badge {badge}'>{stat}</span></td></tr>"
        st.markdown(f'<div class="cbox"><table class="dtbl"><thead><tr><th>Employee</th><th>Dept</th><th>Morning</th><th>Evening</th></tr></thead><tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)
    else:
        st.info("No tasks today. Run morning automation from Scheduler or Owner HQ.")

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: DAILY ENTRY
# ═══════════════════════════════════════════════════════════════════════════
def page_daily_entry():
    cfg  = st.session_state.config
    lang = cfg.get("language", "English")
    t    = T[lang]
    ops  = load_ops()

    st.markdown(f'<div class="ph"><div><div class="ph-title">📝 {t["daily_entry"]}</div><div class="ph-sub">Log production, sales, inventory and expenses</div></div><div class="ph-badge">📅 {datetime.now().strftime("%d %b %Y")}</div></div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["➕ New Entry", "📋 History"])

    with tab1:
        with st.form("daily_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                entry_date  = st.date_input("Date", value=datetime.now())
                production  = st.number_input("Production Quantity (units)", min_value=0, value=0, step=1)
                sales       = st.number_input("Sales Amount (₹)", min_value=0.0, value=0.0, step=500.0)
                expenses    = st.number_input("Expenses (₹)", min_value=0.0, value=0.0, step=500.0)
            with c2:
                orders_rcvd = st.number_input("Orders Received", min_value=0, value=0)
                orders_done = st.number_input("Orders Completed", min_value=0, value=0)
                inquiries   = st.number_input("Customer Inquiries", min_value=0, value=0)
                product     = st.selectbox("Product", cfg.get("products", ["Product A"]))
            inv_note = st.text_input("Inventory Note", placeholder="e.g. Low on Maida...")
            notes    = st.text_area("Notes", placeholder="Issues, highlights...", height=70)
            if st.form_submit_button("💾 Save Entry", use_container_width=True):
                ops.append({
                    "id": f"op_{int(time.time())}", "date": entry_date.strftime("%Y-%m-%d"),
                    "production": production, "sales": sales, "orders_received": orders_rcvd,
                    "orders_completed": orders_done, "expenses": expenses,
                    "inventory_note": inv_note, "customer_inquiries": inquiries,
                    "notes": notes, "product": product,
                })
                save_ops(ops)
                log_activity("Daily entry saved", st.session_state.current_user["name"], str(entry_date))
                st.success(t["entry_saved"])

    with tab2:
        if ops:
            if PANDAS_OK:
                df = pd.DataFrame(ops).sort_values("date", ascending=False)
                cols = ["date", "production", "sales", "orders_received", "orders_completed", "expenses", "inventory_note"]
                df_show = df[[c for c in cols if c in df.columns]].copy()
                df_show["sales"]    = df_show["sales"].apply(lambda x: f"₹{x:,.0f}")
                df_show["expenses"] = df_show["expenses"].apply(lambda x: f"₹{x:,.0f}")
                df_show.columns     = ["Date", "Prod", "Sales", "Orders In", "Done", "Expenses", "Inventory"]
                st.dataframe(df_show, use_container_width=True, hide_index=True)
            st.markdown("<br>", unsafe_allow_html=True)
            a, b, c3 = st.columns(3)
            ts = sum(o.get("sales", 0)      for o in ops)
            ap = sum(o.get("production", 0) for o in ops) // max(len(ops), 1)
            to = sum(o.get("orders_received", 0) for o in ops)
            a.markdown(f'<div class="mc or"><div class="mc-lbl">TOTAL SALES</div><div class="mc-val" style="font-size:1.4rem;">₹{ts:,}</div></div>', unsafe_allow_html=True)
            b.markdown(f'<div class="mc bl"><div class="mc-lbl">AVG PRODUCTION</div><div class="mc-val" style="font-size:1.4rem;">{ap} units</div></div>', unsafe_allow_html=True)
            c3.markdown(f'<div class="mc gr"><div class="mc-lbl">TOTAL ORDERS</div><div class="mc-val" style="font-size:1.4rem;">{to}</div></div>', unsafe_allow_html=True)
        else:
            st.info(t["no_data"])

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: DAILY OPS
# ═══════════════════════════════════════════════════════════════════════════
def page_daily_ops():
    cfg       = st.session_state.config
    lang      = cfg.get("language", "English")
    t         = T[lang]
    today     = datetime.now().strftime("%Y-%m-%d")
    tasks     = load_tasks()
    responses = load_responses()
    ops       = load_ops()

    st.markdown(f'<div class="ph"><div><div class="ph-title">🏭 {t["daily_ops"]}</div><div class="ph-sub">Real-time employee reports and production status</div></div><div class="ph-badge">⚡ Live</div></div>', unsafe_allow_html=True)

    today_tasks = [tk for tk in tasks if tk.get("date") == today]
    today_resps = [r  for r  in responses if r.get("date") == today]
    today_entry = next((o for o in reversed(ops) if o.get("date") == today), None)

    tab1, tab2, tab3, tab4 = st.tabs(["📊 Summary", "👥 Tracker", "💬 Submit Report", "📥 Parsed Data"])

    with tab1:
        sales_r  = [r for r in today_resps if r.get("department") == "Sales"]
        purch_r  = [r for r in today_resps if r.get("department") == "Purchase"]
        store_r  = [r for r in today_resps if r.get("department") == "Storage"]
        ops_r    = [r for r in today_resps if r.get("department") == "Operations"]

        total_s  = sum(r.get("parsed_data", {}).get("sales_amount", 0)     for r in sales_r if r.get("parsed_data"))
        total_p  = sum(r.get("parsed_data", {}).get("production_units", 0) for r in store_r if r.get("parsed_data"))
        total_o  = sum(r.get("parsed_data", {}).get("orders_received", 0)  for r in sales_r if r.get("parsed_data"))
        mandi    = next((r.get("parsed_data", {}).get("mandi_rate", 0)     for r in purch_r if r.get("parsed_data")), 0)

        if today_entry:
            total_s = total_s or today_entry.get("sales", 0)
            total_p = total_p or today_entry.get("production", 0)
            total_o = total_o or today_entry.get("orders_received", 0)

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f'<div class="mc gr"><div class="mc-icon">💰</div><div class="mc-val">₹{total_s:,}</div><div class="mc-lbl">Today Sales</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="mc bl"><div class="mc-icon">🏭</div><div class="mc-val">{total_p}</div><div class="mc-lbl">Production</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="mc or"><div class="mc-icon">📦</div><div class="mc-val">{total_o}</div><div class="mc-lbl">Orders</div></div>', unsafe_allow_html=True)
        c4.markdown(f'<div class="mc pu"><div class="mc-icon">🌾</div><div class="mc-val">₹{mandi:,}</div><div class="mc-lbl">Mandi Rate/Q</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        dc1, dc2, dc3, dc4 = st.columns(4)
        for col, dept, icon, rlist in [(dc1,"Sales","💼",sales_r),(dc2,"Purchase","🌾",purch_r),(dc3,"Storage","📦",store_r),(dc4,"Operations","⚙️",ops_r)]:
            sub  = len(rlist)
            tot  = sum(1 for tk in today_tasks if tk.get("department") == dept)
            badge = "b-ok" if sub == tot and tot > 0 else ("b-warn" if sub > 0 else "b-err")
            col.markdown(f'<div class="cbox" style="text-align:center;"><div style="font-size:1.5rem;">{icon}</div><div style="font-weight:600;margin:.25rem 0;">{dept}</div><span class="badge {badge}">{sub}/{tot} submitted</span></div>', unsafe_allow_html=True)

    with tab2:
        if today_tasks:
            for tk in today_tasks:
                ev     = tk.get("evening_response")
                badge  = "b-ok" if ev else ("b-warn" if tk.get("evening_sent") else "b-err")
                status = "✅ Submitted" if ev else ("⏳ Awaiting" if tk.get("evening_sent") else "❌ Not sent")
                with st.expander(f"{tk['user_name']} — {tk.get('department','?')} — {status}"):
                    col_a, col_b = st.columns(2)
                    col_a.markdown(f"**Morning:** {'✅ Sent' if tk.get('morning_sent') else '❌ Not sent'}")
                    col_b.markdown(f"**Evening:** <span class='badge {badge}'>{status}</span>", unsafe_allow_html=True)
                    if ev:
                        st.markdown(f"> {ev}")
                    if tk.get("parsed_data"):
                        st.json(tk["parsed_data"])
                    if not ev and check_access(["Owner", "Manager"]):
                        with st.form(f"rf_{tk['id']}"):
                            rsp = st.text_area("Simulate employee reply:", placeholder="आज 42000 sales हुई...")
                            if st.form_submit_button("✅ Submit"):
                                simulate_employee_response(tk, rsp, cfg)
                                st.success("Recorded!"); st.rerun()
        else:
            st.info("No tasks today. Run morning automation first.")

    with tab3:
        user = st.session_state.current_user or {}
        my_task = next((tk for tk in tasks if tk.get("user_id") == user.get("id") and tk.get("date") == today), None)
        if user.get("role") == "Employee":
            if my_task:
                if my_task.get("evening_response"):
                    st.success("✅ You already submitted today!")
                    st.markdown(f"**Your response:** {my_task['evening_response']}")
                    if my_task.get("parsed_data"):
                        st.json(my_task["parsed_data"])
                else:
                    dept  = user.get("department", "Operations")
                    tmpls = load_templates()
                    tmpl  = tmpls.get(dept, {})
                    key   = "evening_hi" if lang == "Hindi" else "evening"
                    prompt_text = tmpl.get(key, tmpl.get("evening", "Submit your evening report."))
                    st.markdown(f'<div class="mbox">{prompt_text}</div>', unsafe_allow_html=True)
                    with st.form("self_submit"):
                        response = st.text_area("Your Report:", placeholder="आज 42000 sales हुई, 5 order मिले...", height=110)
                        if st.form_submit_button("📤 Submit Report", use_container_width=True) and response.strip():
                            simulate_employee_response(my_task, response, cfg)
                            st.success("✅ Report submitted!"); st.rerun()
            else:
                st.info("Morning task not assigned. Ask your manager to run morning automation.")
        else:
            st.info("This tab is for employees. Managers use the Tracker tab.")

    with tab4:
        if today_resps:
            for r in reversed(today_resps):
                with st.expander(f"🧠 {r['user_name']} ({r['department']}) — {r.get('timestamp','')[:16].replace('T',' ')}"):
                    col_r, col_p = st.columns(2)
                    col_r.markdown("**Raw:**")
                    col_r.markdown(f'<div class="mbox">{r.get("raw_message","—")}</div>', unsafe_allow_html=True)
                    col_p.markdown("**AI Parsed:**")
                    col_p.json(r.get("parsed_data", {}))
        else:
            st.info("No responses today.")

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: CRM
# ═══════════════════════════════════════════════════════════════════════════
def page_crm():
    cfg  = st.session_state.config
    lang = cfg.get("language", "English")
    t    = T[lang]
    crm  = load_crm()
    user = st.session_state.current_user or {}

    st.markdown(f'<div class="ph"><div><div class="ph-title">👥 {t["crm"]}</div><div class="ph-sub">Customer management, segmentation, follow-ups & communication</div></div><div class="ph-badge">CRM v2</div></div>', unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "➕ Add Lead", "📋 All Leads", "📨 Send Messages", "🤖 AI Follow-ups"])

    with tab1:
        total = len(crm)
        conv  = sum(1 for c in crm if c["status"] == "Converted")
        foll  = sum(1 for c in crm if c["status"] == "Follow-up")
        inter = sum(1 for c in crm if c["status"] == "Interested")
        rate  = int(conv / max(total, 1) * 100)
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.markdown(f'<div class="mc bl"><div class="mc-val">{total}</div><div class="mc-lbl">Total Leads</div></div>', unsafe_allow_html=True)
        k2.markdown(f'<div class="mc gr"><div class="mc-val">{conv}</div><div class="mc-lbl">Converted</div></div>', unsafe_allow_html=True)
        k3.markdown(f'<div class="mc or"><div class="mc-val">{foll}</div><div class="mc-lbl">Follow-up</div></div>', unsafe_allow_html=True)
        k4.markdown(f'<div class="mc pu"><div class="mc-val">{inter}</div><div class="mc-lbl">Interested</div></div>', unsafe_allow_html=True)
        k5.markdown(f'<div class="mc gr"><div class="mc-val">{rate}%</div><div class="mc-lbl">Conv. Rate</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        today_str = datetime.now().strftime("%Y-%m-%d")
        overdue = [c for c in crm if c.get("follow_up_date") and c["follow_up_date"] <= today_str
                   and c["status"] not in ["Converted", "Lost"]]
        if overdue:
            st.warning(f"⚠️ {len(overdue)} follow-up(s) overdue:")
            for c in overdue[:5]:
                st.markdown(f'<div class="cbox" style="padding:.7rem 1rem;margin-bottom:.4rem;"><strong>{c["name"]}</strong> <span class="badge b-warn">{c["segment"]}</span> — {c.get("phone","—")} — <span style="color:#EF4444;font-size:.8rem;">Due: {c.get("follow_up_date","?")}</span></div>', unsafe_allow_html=True)

    with tab2:
        with st.form("crm_add", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                name    = st.text_input("Customer Name")
                phone   = st.text_input("Phone")
                segment = st.selectbox("Segment", ["Retail", "Bulk", "Distributor"])
            with c2:
                status     = st.selectbox("Status", ["Interested", "Follow-up", "Converted", "Lost"])
                fdate      = st.date_input("Follow-up Date", value=datetime.now() + timedelta(days=3))
                assigned   = st.text_input("Assigned To", value=user.get("name", ""))
            requirement = st.text_area("Requirement", height=65)
            notes       = st.text_area("Notes", height=55)
            if st.form_submit_button("➕ Add Lead", use_container_width=True) and name:
                crm.append({
                    "id": f"crm_{int(time.time())}", "name": name, "phone": phone,
                    "requirement": requirement, "status": status, "segment": segment,
                    "last_contact": datetime.now().strftime("%Y-%m-%d"),
                    "follow_up_date": fdate.strftime("%Y-%m-%d"), "notes": notes,
                    "created": datetime.now().strftime("%Y-%m-%d"),
                    "assigned_to": assigned, "comm_log": [],
                })
                save_crm(crm)
                log_activity(f"CRM lead added:{name}", user.get("name", ""))
                st.success(f"✅ '{name}' added!"); st.rerun()

    with tab3:
        f1, f2, f3 = st.columns([2, 1, 1])
        with f1: search  = st.text_input("🔍 Search", placeholder="Name, phone...")
        with f2: fstatus = st.selectbox("Status", ["All","Interested","Follow-up","Converted","Lost"])
        with f3: fseg    = st.selectbox("Segment", ["All","Retail","Bulk","Distributor"])

        fil = crm
        if search:
            s = search.lower()
            fil = [c for c in fil if s in c.get("name","").lower() or s in c.get("phone","")]
        if fstatus != "All": fil = [c for c in fil if c.get("status") == fstatus]
        if fseg    != "All": fil = [c for c in fil if c.get("segment") == fseg]

        bm = {"Converted":"b-conv","Follow-up":"b-foll","Interested":"b-int","Lost":"b-lost"}
        sm = {"Bulk":"b-info","Retail":"b-ok","Distributor":"b-pu"}

        if fil:
            rows = ""
            for c in reversed(fil):
                bc = bm.get(c.get("status",""),"b-int")
                bs = sm.get(c.get("segment",""),"b-info")
                rows += f"<tr><td><strong>{c.get('name','—')}</strong></td><td>{c.get('phone','—')}</td><td><span class='badge {bs}'>{c.get('segment','—')}</span></td><td><span class='badge {bc}'>{c.get('status','—')}</span></td><td style='color:#8B949E;font-size:.78rem;'>{c.get('follow_up_date','—')}</td><td style='color:#8B949E;font-size:.78rem;'>{c.get('requirement','')[:35]}</td></tr>"
            st.markdown(f'<div class="cbox"><table class="dtbl"><thead><tr><th>Name</th><th>Phone</th><th>Segment</th><th>Status</th><th>Follow-up</th><th>Requirement</th></tr></thead><tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)
            st.caption(f"Showing {len(fil)} of {len(crm)} leads")

            if check_access(["Owner", "Manager"]):
                with st.expander("✏️ Quick Update"):
                    with st.form("crm_update"):
                        sel  = st.selectbox("Lead", [c["name"] for c in fil])
                        ns   = st.selectbox("New Status", ["Interested","Follow-up","Converted","Lost"])
                        nn   = st.text_input("Note")
                        nfd  = st.date_input("New Follow-up", value=datetime.now() + timedelta(days=3))
                        if st.form_submit_button("Update"):
                            for c in crm:
                                if c["name"] == sel:
                                    c["status"] = ns; c["last_contact"] = datetime.now().strftime("%Y-%m-%d")
                                    c["follow_up_date"] = nfd.strftime("%Y-%m-%d")
                                    if nn: c["notes"] = (c.get("notes","") + f"\n[{datetime.now().strftime('%d/%m')}] {nn}").strip()
                                    c.setdefault("comm_log",[]).append({"date":datetime.now().isoformat(),"note":nn,"status":ns})
                            save_crm(crm); st.success("✅ Updated!"); st.rerun()
        else:
            st.info("No leads found.")

    with tab4:
        c1, c2 = st.columns(2)
        with c1:
            send_to = st.selectbox("Send to", ["Specific Customer","All Follow-up","All Converted","All Bulk"])
            if   send_to == "Specific Customer": targets = [c for c in crm if c["name"] == st.selectbox("Customer", [c["name"] for c in crm])]
            elif send_to == "All Follow-up":     targets = [c for c in crm if c["status"] == "Follow-up"]
            elif send_to == "All Converted":     targets = [c for c in crm if c["status"] == "Converted"]
            else:                                targets = [c for c in crm if c.get("segment") == "Bulk"]
            st.caption(f"{len(targets)} recipient(s)")
            msg_type = st.selectbox("Type", ["Promotional Offer","Payment Reminder","Order Update","Festival Greetings","Custom"])
            channel  = st.radio("Channel", ["WhatsApp","Email"], horizontal=True)

        with c2:
            custom_msg = st.text_area("Message", height=130, placeholder="नमस्ते {name}! ...")
            if st.button("🤖 AI Generate"):
                prompt = f"Write a {msg_type} message for {cfg['business_type']} to customers. Business: {cfg['business_name']}. Lang: {lang}. Max 100 words."
                with st.spinner("Generating..."):
                    st.session_state["crm_msg"] = call_ai(prompt, build_system_prompt(cfg), cfg.get("api_key",""), lang)
                st.rerun()
            if "crm_msg" in st.session_state:
                st.markdown(f'<div class="mbox">{st.session_state["crm_msg"]}</div>', unsafe_allow_html=True)

        final = st.session_state.get("crm_msg","") or custom_msg
        if st.button(f"📤 Send to {len(targets)} Customer(s)", use_container_width=True) and final and targets:
            for tgt in targets:
                txt = final.replace("{name}", tgt["name"].split()[0])
                send_message(tgt["name"], tgt.get("phone",""), txt, channel, "crm", user.get("name",""))
                for c in crm:
                    if c["id"] == tgt["id"]:
                        c["last_contact"] = datetime.now().strftime("%Y-%m-%d")
                        c.setdefault("comm_log",[]).append({"date":datetime.now().isoformat(),"channel":channel,"message":txt[:80]})
            save_crm(crm)
            st.success(f"✅ Sent to {len(targets)} customer(s)!")
            st.session_state.pop("crm_msg", None)

    with tab5:
        foll_leads = [c for c in crm if c.get("status") in ["Interested","Follow-up"]]
        if foll_leads:
            if st.button("🔄 Generate AI Suggestions", use_container_width=True):
                leads_txt = "\n".join([f"- {c['name']} ({c.get('phone','')}) — {c.get('segment','')} — {c.get('requirement','')} — Last: {c.get('last_contact','?')}" for c in foll_leads])
                prompt = f"CRM follow-up suggestions for:\n{leads_txt}\n\nFor each: priority, method, message template ({lang}), objection + handling."
                with st.spinner(t["ai_thinking"]):
                    resp = call_ai(prompt, build_system_prompt(cfg), cfg.get("api_key",""), lang)
                st.markdown(f'<div class="rbox">{resp.replace(chr(10),"<br>")}</div>', unsafe_allow_html=True)
        else:
            st.info("No Follow-up/Interested leads.")

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: WORKFORCE
# ═══════════════════════════════════════════════════════════════════════════
def page_workforce():
    if not check_access(["Owner","Manager"]):
        st.warning("⛔ Manager/Owner access only."); return
    cfg  = st.session_state.config
    user = st.session_state.current_user or {}

    st.markdown('<div class="ph"><div><div class="ph-title">👤 Workforce</div><div class="ph-sub">Manage employees, suppliers, distributors</div></div><div class="ph-badge">👥 Contacts</div></div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["👤 Users","🤝 Suppliers & Distributors","➕ Add"])

    with tab1:
        users = load_users()
        rows  = ""
        for u in users:
            rc = {"Owner":"b-info","Manager":"b-warn","Employee":"b-ok"}.get(u.get("role",""),"b-info")
            rows += f"<tr><td><strong>{u['name']}</strong></td><td>{u.get('phone','—')}</td><td><span class='badge {rc}'>{u.get('role','—')}</span></td><td>{u.get('department','—')}</td><td>{u.get('preferred_language','EN')}</td><td>{'🟢' if u.get('active') else '🔴'}</td></tr>"
        st.markdown(f'<div class="cbox"><table class="dtbl"><thead><tr><th>Name</th><th>Phone</th><th>Role</th><th>Dept</th><th>Lang</th><th>Active</th></tr></thead><tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)

        if check_access(["Owner"]):
            with st.expander("➕ Add User"):
                with st.form("add_user"):
                    uc1, uc2 = st.columns(2)
                    with uc1:
                        un = st.text_input("Name"); up_h = st.text_input("Phone"); ur = st.selectbox("Role",["Owner","Manager","Employee"])
                    with uc2:
                        ud = st.selectbox("Dept",["Sales","Purchase","Storage","Operations","Management"]); ul = st.selectbox("Lang",["English","Hindi"]); upw = st.text_input("Password",type="password")
                    if st.form_submit_button("Add") and un:
                        users.append({"id":f"u{int(time.time())}","name":un,"phone":up_h,"role":ur,"department":ud,"preferred_language":ul,"password":hash_pw(upw),"active":True})
                        save_users(users); st.success(f"✅ {un} added!"); st.rerun()

    with tab2:
        contacts = load_contacts()
        ext = [c for c in contacts if c.get("type") in ["supplier","distributor"]]
        rows = ""
        for c in ext:
            tb = {"supplier":"b-info","distributor":"b-pu"}.get(c.get("type",""),"b-info")
            rows += f"<tr><td><strong>{c['name']}</strong></td><td>{c.get('phone','—')}</td><td><span class='badge {tb}'>{c.get('type','').title()}</span></td><td>{c.get('assigned_manager','—')}</td><td style='color:#8B949E;font-size:.78rem;'>{c.get('last_contact','—')}</td></tr>"
        st.markdown(f'<div class="cbox"><table class="dtbl"><thead><tr><th>Name</th><th>Phone</th><th>Type</th><th>Manager</th><th>Last Contact</th></tr></thead><tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)

    with tab3:
        contacts = load_contacts()
        with st.form("add_contact"):
            cc1, cc2 = st.columns(2)
            with cc1:
                cn = st.text_input("Name"); cp = st.text_input("Phone"); ct = st.selectbox("Type",["customer","supplier","distributor"])
            with cc2:
                cr_role = st.text_input("Role/Title"); cm = st.text_input("Assigned Manager"); cl = st.selectbox("Language",["Hindi","English"])
            cno = st.text_area("Notes",height=55)
            if st.form_submit_button("➕ Add Contact") and cn:
                contacts.append({"id":f"c{int(time.time())}","name":cn,"phone":cp,"type":ct,"role":cr_role,"assigned_manager":cm,"preferred_language":cl,"active_status":True,"last_contact":datetime.now().strftime("%Y-%m-%d"),"notes":cno})
                save_contacts(contacts); st.success(f"✅ {cn} added!"); st.rerun()

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════
def page_scheduler():
    if not check_access(["Owner","Manager"]):
        st.warning("⛔ Manager/Owner access only."); return
    cfg  = st.session_state.config
    user = st.session_state.current_user or {}
    sched_cfg = load_schedule()

    st.markdown('<div class="ph"><div><div class="ph-title">⏰ Scheduler</div><div class="ph-sub">Automate morning tasks and evening report requests</div></div><div class="ph-badge">⚡ Auto</div></div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["⏰ Config","📋 Templates","📜 History"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="cbox">', unsafe_allow_html=True)
            st.markdown("### 🌅 Morning Messages")
            m_en = st.checkbox("Enable", value=sched_cfg.get("morning_enabled", True), key="men")
            m_tm = st.text_input("Time (HH:MM)", value=sched_cfg.get("morning_time","09:00"), key="mtm")
            st.caption(f"Last run: {sched_cfg.get('last_morning_run','Never')}")
            st.markdown('</div>', unsafe_allow_html=True)
        with c2:
            st.markdown('<div class="cbox">', unsafe_allow_html=True)
            st.markdown("### 🌆 Evening Reports")
            e_en = st.checkbox("Enable", value=sched_cfg.get("evening_enabled", True), key="een")
            e_tm = st.text_input("Time (HH:MM)", value=sched_cfg.get("evening_time","19:00"), key="etm")
            st.caption(f"Last run: {sched_cfg.get('last_evening_run','Never')}")
            st.markdown('</div>', unsafe_allow_html=True)

        if st.button("💾 Save Schedule", use_container_width=True):
            ns = {**sched_cfg,"morning_enabled":m_en,"morning_time":m_tm,"evening_enabled":e_en,"evening_time":e_tm}
            save_schedule(ns)
            if SCHEDULER_OK: apply_schedule(ns)
            st.success("✅ Schedule saved!" + (" APScheduler active." if SCHEDULER_OK else " (Install apscheduler for auto-run)"))

        st.markdown("<br>**Manual Triggers:**")
        tc1, tc2 = st.columns(2)
        with tc1:
            if st.button("🌅 Send Morning Messages NOW", use_container_width=True):
                with st.spinner("Sending..."):
                    _, cnt = run_daily_automation("morning", user.get("name",""))
                    sched_cfg["last_morning_run"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    save_schedule(sched_cfg)
                st.success(f"✅ Sent to {cnt} users!")
        with tc2:
            if st.button("🌆 Request Evening Reports NOW", use_container_width=True):
                with st.spinner("Sending..."):
                    _, cnt = run_daily_automation("evening", user.get("name",""))
                    sched_cfg["last_evening_run"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    save_schedule(sched_cfg)
                st.success(f"✅ Sent to {cnt} users!")

        if not SCHEDULER_OK:
            st.warning("⚠️ `pip install apscheduler` for automatic scheduling. Manual triggers work fine.")

    with tab2:
        templates = load_templates()
        sel = st.selectbox("Department", list(templates.keys()))
        if sel:
            dept = templates[sel]
            t1, t2 = st.columns(2)
            with t1:
                st.markdown("**Morning (English)**")
                me = st.text_area("", value=dept.get("morning",""), height=160, key=f"me_{sel}")
                st.markdown("**Morning (Hindi)**")
                mh = st.text_area("", value=dept.get("morning_hi",""), height=160, key=f"mh_{sel}")
            with t2:
                st.markdown("**Evening (English)**")
                ee = st.text_area("", value=dept.get("evening",""), height=160, key=f"ee_{sel}")
                st.markdown("**Evening (Hindi)**")
                eh = st.text_area("", value=dept.get("evening_hi",""), height=160, key=f"eh_{sel}")
            if st.button(f"💾 Save {sel} Templates", use_container_width=True):
                templates[sel] = {"morning":me,"morning_hi":mh,"evening":ee,"evening_hi":eh}
                save_templates(templates); st.success("✅ Saved!")

    with tab3:
        msgs = load_messages()
        if msgs:
            if PANDAS_OK:
                df = pd.DataFrame(reversed(msgs[-50:]))
                show_cols = [c for c in ["timestamp","recipient","channel","type","status"] if c in df.columns]
                df["timestamp"] = df["timestamp"].str[:16].str.replace("T"," ")
                st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No messages sent yet.")

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: REPORTS
# ═══════════════════════════════════════════════════════════════════════════
def page_reports():
    cfg       = st.session_state.config
    lang      = cfg.get("language","English")
    t         = T[lang]
    ops       = load_ops()
    crm       = load_crm()
    responses = load_responses()
    user      = st.session_state.current_user or {}

    st.markdown(f'<div class="ph"><div><div class="ph-title">📈 {t["reports"]}</div><div class="ph-sub">AI-powered reports — analytics, trends and recommendations</div></div><div class="ph-badge">📊 Analytics</div></div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🤖 AI Report","📊 Charts","📨 Messages"])

    with tab1:
        rc1, rc2 = st.columns([1, 2])
        with rc1:
            period   = st.selectbox("Period", ["Today","Last 7 Days","Last 30 Days"])
            incl_emp = st.checkbox("Include Employee Reports", value=True)
            incl_crm = st.checkbox("Include CRM Summary",     value=True)
            incl_sug = st.checkbox("Include Recommendations", value=True)
            gen      = st.button("🚀 Generate AI Report", use_container_width=True)
        with rc2:
            if gen:
                today = datetime.now().strftime("%Y-%m-%d")
                if period == "Today":
                    filtered = [o for o in ops if o.get("date") == today]; label = "Today"
                elif period == "Last 7 Days":
                    dates = [(datetime.now()-timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
                    filtered = [o for o in ops if o.get("date") in dates]; label = "Last 7 Days"
                else:
                    dates = [(datetime.now()-timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]
                    filtered = [o for o in ops if o.get("date") in dates]; label = "Last 30 Days"

                if not filtered:
                    st.warning("No data for this period.")
                else:
                    n  = max(len(filtered), 1)
                    ts = sum(o.get("sales",0) for o in filtered)
                    tp = sum(o.get("production",0) for o in filtered)
                    te = sum(o.get("expenses",0) for o in filtered)
                    tr = sum(o.get("orders_received",0) for o in filtered)
                    tc = sum(o.get("orders_completed",0) for o in filtered)
                    crm_text = f"\nCRM: {len(crm)} leads, {sum(1 for c in crm if c['status']=='Converted')} converted, {sum(1 for c in crm if c['status']=='Follow-up')} follow-ups." if incl_crm else ""
                    emp_resps = [r for r in responses if r.get("date") == today] if incl_emp else []
                    emp_text  = f"\nEmployee responses: {len(emp_resps)} from {', '.join(set(r['department'] for r in emp_resps))}." if emp_resps else ""
                    prompt = f"""Business report for {label}:
Business: {cfg['business_name']} ({cfg['business_type']}) — {cfg.get('city','')}
Sales: ₹{ts:,} | Avg: ₹{ts//n:,}/day | Target: ₹{cfg.get('target_daily_sales',50000):,}/day
Production: {tp} units | Avg: {tp//n}/day | Target: {cfg.get('target_daily_production',200)}/day
Expenses: ₹{te:,} | Profit est: ₹{ts-te:,}
Orders: {tr} received | {tc} done | Fulfillment: {int(tc/max(tr,1)*100)}%{crm_text}{emp_text}
Sections: Executive Summary, Financial, Operations, Alerts.{"Recommendations." if incl_sug else ""}"""
                    with st.spinner(t["ai_thinking"]):
                        rpt = call_ai(prompt, build_system_prompt(cfg), cfg.get("api_key",""), lang)
                    st.markdown(f'<div class="rbox">{rpt.replace(chr(10),"<br>")}</div>', unsafe_allow_html=True)

    with tab2:
        if ops and PLOTLY_OK and PANDAS_OK:
            df = pd.DataFrame(ops)
            df["date"] = pd.to_datetime(df["date"])
            c1, c2 = st.columns(2)
            with c1:
                fig = px.line(df, x="date", y="sales", title="Sales Trend", color_discrete_sequence=["#F97316"])
                fig.update_layout(plot_bgcolor="#161B22",paper_bgcolor="#161B22",font_color="#8B949E",height=265,margin=dict(l=0,r=0,t=40,b=0))
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                fig2 = px.bar(df, x="date", y="production", title="Production Volume", color_discrete_sequence=["#3B82F6"])
                fig2.update_layout(plot_bgcolor="#161B22",paper_bgcolor="#161B22",font_color="#8B949E",height=265,margin=dict(l=0,r=0,t=40,b=0))
                st.plotly_chart(fig2, use_container_width=True)
            c3, c4 = st.columns(2)
            with c3:
                fig3 = px.area(df, x="date", y="expenses", title="Expenses", color_discrete_sequence=["#EF4444"])
                fig3.update_layout(plot_bgcolor="#161B22",paper_bgcolor="#161B22",font_color="#8B949E",height=265,margin=dict(l=0,r=0,t=40,b=0))
                st.plotly_chart(fig3, use_container_width=True)
            with c4:
                if "product" in df.columns:
                    ps = df.groupby("product")["sales"].sum().reset_index()
                    fig4 = px.pie(ps, names="product", values="sales", title="By Product",
                                  color_discrete_sequence=["#F97316","#3B82F6","#10B981"])
                    fig4.update_layout(plot_bgcolor="#161B22",paper_bgcolor="#161B22",font_color="#8B949E",height=265,margin=dict(l=0,r=0,t=40,b=0))
                    st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info(t["no_data"])

    with tab3:
        c1, c2 = st.columns(2)
        with c1:
            msg_type  = st.selectbox("Type",["Daily Report","Customer Follow-up","Payment Reminder","Custom"])
            recipient = st.text_input("Recipient")
            channel   = st.radio("Channel",["WhatsApp","Email"], horizontal=True)
        with c2:
            if st.button("🤖 AI Generate Message"):
                prompt = f"Write {msg_type} for {cfg['business_name']}. Lang:{lang}. Max 120 words."
                with st.spinner("..."):
                    st.session_state["rpt_msg"] = call_ai(prompt, build_system_prompt(cfg), cfg.get("api_key",""), lang)
                st.rerun()
            if "rpt_msg" in st.session_state:
                st.markdown(f'<div class="mbox">{st.session_state["rpt_msg"]}</div>', unsafe_allow_html=True)
        if st.button("📤 Send (Simulate)", use_container_width=True):
            content = st.session_state.get("rpt_msg","")
            if content and recipient:
                send_message(recipient, "", content, channel, msg_type, user.get("name",""))
                st.success(f"✅ Simulated for {recipient}")

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: AI ASSISTANT
# ═══════════════════════════════════════════════════════════════════════════
def page_ai_assistant():
    cfg  = st.session_state.config
    lang = cfg.get("language","English")
    t    = T[lang]
    ops  = load_ops()
    crm  = load_crm()
    tasks = load_tasks()

    st.markdown(f'<div class="ph"><div><div class="ph-title">🤖 {t["ai_assistant"]}</div><div class="ph-sub">Ask anything in English or Hindi — understands your business</div></div><div class="ph-badge">⚡ AI</div></div>', unsafe_allow_html=True)

    quick = [
        ("📊 Today Summary",   "Give complete summary of today's operations, sales, employee reports"),
        ("💰 Sales Analysis",  "Analyze last 7 days sales, trends, compare with targets"),
        ("👥 CRM Status",      "Summarize customer pipeline, who to follow up urgently"),
        ("📦 Inventory",       "Check inventory notes, alert about low stock items"),
        ("💡 Growth Tips",     "Give 5 actionable strategies to grow revenue based on data"),
        ("⚠️ Risk Check",      "Identify operational risks from recent employee reports"),
        ("🧑 Employee Report", "Summarize employee performance — who submitted, who didn't"),
        ("📈 Weekly Goals",    "Set realistic targets for next week based on performance"),
    ]

    def build_context():
        last7 = ops[-7:] if len(ops) >= 7 else ops
        today = datetime.now().strftime("%Y-%m-%d")
        tt    = [tk for tk in tasks if tk.get("date") == today]
        return (f"Ops last {len(last7)} days: "
                f"{json.dumps([{k:v for k,v in o.items() if k in ['date','production','sales','orders_received','orders_completed','expenses','inventory_note']} for o in last7], ensure_ascii=False)}\n"
                f"CRM: {len(crm)} leads | Conv:{sum(1 for c in crm if c['status']=='Converted')} | FU:{sum(1 for c in crm if c['status']=='Follow-up')}\n"
                f"Employee tasks today: {len(tt)} | Submitted:{sum(1 for tk in tt if tk.get('evening_response'))}\n"
                f"Targets: Sales ₹{cfg.get('target_daily_sales',50000):,}/day | Prod {cfg.get('target_daily_production',200)}/day")

    qcols1 = st.columns(4)
    for i, (label, prompt) in enumerate(quick[:4]):
        with qcols1[i]:
            if st.button(label, use_container_width=True, key=f"qp{i}"):
                st.session_state.chat_history.append({"role":"user","content":prompt})
                with st.spinner(t["ai_thinking"]):
                    resp = call_ai(f"{build_context()}\n\n{prompt}", build_system_prompt(cfg), cfg.get("api_key",""), lang)
                st.session_state.chat_history.append({"role":"assistant","content":resp})
                st.rerun()
    qcols2 = st.columns(4)
    for i, (label, prompt) in enumerate(quick[4:]):
        with qcols2[i]:
            if st.button(label, use_container_width=True, key=f"qp2_{i}"):
                st.session_state.chat_history.append({"role":"user","content":prompt})
                with st.spinner(t["ai_thinking"]):
                    resp = call_ai(f"{build_context()}\n\n{prompt}", build_system_prompt(cfg), cfg.get("api_key",""), lang)
                st.session_state.chat_history.append({"role":"assistant","content":resp})
                st.rerun()

    st.markdown("<hr>", unsafe_allow_html=True)

    if not st.session_state.chat_history:
        st.markdown(f"""
        <div style="text-align:center;padding:2.5rem 0;color:#8B949E;">
          <div style="font-size:3rem;margin-bottom:.7rem;">🤖</div>
          <div style="font-size:1rem;font-weight:600;color:#F0F6FC;">
            {'Ask me anything about your business, CRM, or employee reports!' if lang=='English'
             else 'व्यवसाय, CRM, या कर्मचारी रिपोर्ट के बारे में कुछ भी पूछें!'}
          </div>
          <div style="font-size:.82rem;margin-top:.35rem;">
            {"Try: 'आज की बिक्री कितनी हुई?' or 'Who hasn't submitted their report?'" if lang=='English'
             else "उदाहरण: 'आज का उत्पादन?' या 'किसने रिपोर्ट नहीं भेजी?'"}
          </div>
        </div>""", unsafe_allow_html=True)
    else:
        for msg in st.session_state.chat_history:
            if msg["role"] == "user":
                st.markdown(f'<div class="chat-user">👤 {msg["content"]}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="chat-ai"><div class="chat-hdr">🚀 BizPilot AI</div>{msg["content"].replace(chr(10),"<br>")}</div>', unsafe_allow_html=True)

    with st.form("chat_form", clear_on_submit=True):
        ci, cb = st.columns([5, 1])
        with ci:
            user_input = st.text_input("", placeholder="Ask anything... / कुछ भी पूछें...", label_visibility="collapsed")
        with cb:
            send = st.form_submit_button("Send 🚀", use_container_width=True)
        if send and user_input.strip():
            st.session_state.chat_history.append({"role":"user","content":user_input})
            with st.spinner(t["ai_thinking"]):
                resp = call_ai(f"{build_context()}\n\nUser: {user_input}", build_system_prompt(cfg), cfg.get("api_key",""), lang)
            st.session_state.chat_history.append({"role":"assistant","content":resp})
            st.rerun()

    if st.session_state.chat_history:
        if st.button("🗑️ Clear Chat"):
            st.session_state.chat_history = []
            st.rerun()

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: OWNER HQ
# ═══════════════════════════════════════════════════════════════════════════
def page_owner_hq():
    if not check_access(["Owner"]):
        st.warning("⛔ Owner access only."); return
    cfg   = st.session_state.config
    lang  = cfg.get("language","English")
    user  = st.session_state.current_user or {}
    ops   = load_ops(); crm = load_crm(); tasks = load_tasks(); users = load_users()
    today = datetime.now().strftime("%Y-%m-%d")
    tt    = [tk for tk in tasks if tk.get("date") == today]
    sub   = [tk for tk in tt if tk.get("evening_response")]
    nosub = [tk for tk in tt if not tk.get("evening_response")]
    last7 = ops[-7:] if len(ops) >= 7 else ops; n = max(len(last7), 1)
    ts = sum(o.get("sales",0) for o in last7); tp = sum(o.get("production",0) for o in last7)
    te = sum(o.get("expenses",0) for o in last7); tr = sum(o.get("orders_received",0) for o in last7); tc = sum(o.get("orders_completed",0) for o in last7)
    conv = sum(1 for c in crm if c["status"]=="Converted"); conv_r = int(conv/max(len(crm),1)*100)

    st.markdown('<div class="ph"><div><div class="ph-title">👑 Owner HQ</div><div class="ph-sub">Complete business control — automation, performance, alerts</div></div><div class="ph-badge">👑 Owner Only</div></div>', unsafe_allow_html=True)

    bc1, bc2, bc3, bc4 = st.columns(4)
    with bc1:
        if st.button("🌅 Morning Messages", use_container_width=True):
            with st.spinner("Sending..."):
                _, cnt = run_daily_automation("morning", user.get("name",""))
            st.success(f"✅ {cnt} sent!")
    with bc2:
        if st.button("🌆 Evening Reports", use_container_width=True):
            with st.spinner("Sending..."):
                _, cnt = run_daily_automation("evening", user.get("name",""))
            st.success(f"✅ {cnt} sent!")
    with bc3:
        if st.button("⚠️ Send Reminders", use_container_width=True):
            for tk in nosub:
                u2 = next((u for u in users if u["id"] == tk.get("user_id")), None)
                if u2:
                    send_message(u2["name"], u2.get("phone",""), f"📌 Reminder: Please submit your evening report! — BizPilot AI", "WhatsApp","reminder",user.get("name",""))
            st.success(f"✅ {len(nosub)} reminders sent!")
    with bc4:
        if st.button("📊 Full Report →", use_container_width=True):
            st.session_state.page = "Reports"; st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="sh">📊 7-Day Performance</div>', unsafe_allow_html=True)
    k1,k2,k3,k4,k5,k6 = st.columns(6)
    for col,val,lbl,cls in [(k1,f"₹{ts:,}","7d Sales","or"),(k2,f"{tp}","7d Prod","bl"),(k3,f"₹{te:,}","7d Expenses","re"),(k4,f"₹{ts-te:,}","Est. Profit","gr"),(k5,f"{int(tc/max(tr,1)*100)}%","Fulfillment","pu"),(k6,f"{conv_r}%","CRM Conv.","gr")]:
        col.markdown(f'<div class="mc {cls}"><div class="mc-val" style="font-size:1.25rem;">{val}</div><div class="mc-lbl">{lbl}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    oc1, oc2 = st.columns(2)

    with oc1:
        st.markdown('<div class="sh">👥 Employee Accountability</div>', unsafe_allow_html=True)
        if tt:
            rows = ""
            for tk in tt:
                ev = tk.get("evening_response")
                badge = "b-ok" if ev else "b-err"
                rows += f"<tr><td><strong>{tk['user_name']}</strong></td><td>{tk.get('department','')}</td><td><span class='badge {badge}'>{'✅ Done' if ev else '❌ Pending'}</span></td></tr>"
            st.markdown(f'<div class="cbox"><table class="dtbl"><thead><tr><th>Employee</th><th>Dept</th><th>Report</th></tr></thead><tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)
            st.markdown(f"**✅ {len(sub)} submitted | ❌ {len(nosub)} pending**")
        else:
            st.info("Run morning automation to create tasks.")

    with oc2:
        st.markdown('<div class="sh">🚨 Alerts</div>', unsafe_allow_html=True)
        alerts = []
        if last7 and ts/n < cfg.get("target_daily_sales",50000) * 0.8:
            alerts.append(("#EF4444","🔴 Sales Alert",f"Avg ₹{ts//n:,}/day is 20%+ below target"))
        if len(nosub) > len(tt) * 0.5 and tt:
            alerts.append(("#FBBF24","🟡 Reports Pending",f"{len(nosub)}/{len(tt)} not submitted"))
        today_str = datetime.now().strftime("%Y-%m-%d")
        ov = sum(1 for c in crm if c.get("follow_up_date","9999") <= today_str and c["status"] not in ["Converted","Lost"])
        if ov: alerts.append(("#FBBF24","🟡 CRM Follow-ups",f"{ov} leads overdue"))
        if any("low" in o.get("inventory_note","").lower() for o in ops[-3:] if o.get("inventory_note")):
            alerts.append(("#EF4444","🔴 Inventory",  "Low stock reported recently"))
        if not alerts: alerts.append(("#10B981","🟢 All Clear","No major alerts"))
        for color, title, msg in alerts:
            st.markdown(f'<div class="cbox" style="border-left:3px solid {color};margin-bottom:.45rem;"><div style="color:{color};font-weight:600;">{title}</div><div style="color:#8B949E;font-size:.83rem;">{msg}</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="sh">🤖 AI Owner Briefing</div>', unsafe_allow_html=True)
    if st.button("🔄 Generate Briefing", use_container_width=True):
        foll = sum(1 for c in crm if c["status"]=="Follow-up")
        prompt = f"""Owner briefing for {cfg['business_name']} ({lang}):
7-day sales: ₹{ts:,} | Target: ₹{cfg.get('target_daily_sales',50000)*7:,}
Production: {tp} | Expenses: ₹{te:,} | Profit: ₹{ts-te:,}
Fulfillment: {int(tc/max(tr,1)*100)}% | CRM conv: {conv_r}% | {foll} follow-ups needed
Reports today: {len(sub)}/{len(tt)}
Include: What went well, attention needed, top 3 actions for tomorrow."""
        with st.spinner("Generating..."):
            brief = call_ai(prompt, build_system_prompt(cfg), cfg.get("api_key",""), lang)
        st.markdown(f'<div class="rbox">{brief.replace(chr(10),"<br>")}</div>', unsafe_allow_html=True)

    msgs = load_messages()
    today_msgs = [m for m in msgs if m.get("timestamp","").startswith(today)]
    if today_msgs:
        st.markdown(f"<br>**📨 Today's Messages: {len(today_msgs)}**", unsafe_allow_html=True)
        if PANDAS_OK:
            df = pd.DataFrame(reversed(today_msgs[-15:]))[["timestamp","recipient","type","channel","status"]]
            df["timestamp"] = df["timestamp"].str[:16].str.replace("T"," ")
            st.dataframe(df, use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: ACTIVITY LOG
# ═══════════════════════════════════════════════════════════════════════════
def page_activity():
    st.markdown('<div class="ph"><div><div class="ph-title">📋 Activity Log</div><div class="ph-sub">Complete audit trail — logins, messages, reports, CRM updates</div></div><div class="ph-badge">🔍 Audit</div></div>', unsafe_allow_html=True)

    activity = load_activity()
    if not activity:
        st.info("No activity logged yet."); return

    f1, f2 = st.columns(2)
    with f1: srch = st.text_input("🔍 Search", placeholder="user, action...")
    with f2: af   = st.selectbox("Filter", ["All","Login","Logout","Message","Daily entry","CRM"])

    fil = activity
    if srch:
        s = srch.lower()
        fil = [a for a in fil if s in a.get("user","").lower() or s in a.get("action","").lower()]
    if af != "All":
        fil = [a for a in fil if af.lower() in a.get("action","").lower()]

    st.caption(f"Showing {len(fil)} of {len(activity)} entries")
    rows = ""
    for a in reversed(fil[-100:]):
        ts = a.get("timestamp","")[:16].replace("T"," ")
        ac = a.get("action","")
        color = "#10B981" if "login" in ac.lower() else ("#EF4444" if "logout" in ac.lower() else ("#3B82F6" if "message" in ac.lower() else "#8B949E"))
        rows += f"<tr><td style='color:#8B949E;font-size:.78rem;'>{ts}</td><td style='color:#F97316;font-weight:500;'>{a.get('user','system')}</td><td style='color:{color};'>{ac}</td><td style='color:#8B949E;font-size:.8rem;'>{a.get('detail','')[:60]}</td></tr>"
    st.markdown(f'<div class="cbox"><table class="dtbl"><thead><tr><th>Time</th><th>User</th><th>Action</th><th>Detail</th></tr></thead><tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)

    if check_access(["Owner"]) and st.button("🗑️ Clear Log"):
        save_activity([]); st.success("Cleared."); st.rerun()

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
def page_settings():
    cfg  = st.session_state.config
    lang = cfg.get("language","English")
    user = st.session_state.current_user or {}

    st.markdown('<div class="ph"><div><div class="ph-title">⚙️ Settings</div><div class="ph-sub">Configure BizPilot AI for your business</div></div><div class="ph-badge">🔧 Config</div></div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🏢 Business","🤖 AI","🗄️ Data"])

    with tab1:
        with st.form("settings_form"):
            c1, c2 = st.columns(2)
            with c1:
                bn = st.text_input("Business Name", value=cfg.get("business_name",""))
                on = st.text_input("Owner Name",    value=cfg.get("owner_name",""))
                ci = st.text_input("City",          value=cfg.get("city","Indore"))
            with c2:
                bt_opts = ["Manufacturing / Distribution","Retail","Services","Trading","Food & Beverage","Other"]
                bt = st.selectbox("Business Type", bt_opts, index=bt_opts.index(cfg.get("business_type","Manufacturing / Distribution")) if cfg.get("business_type") in bt_opts else 0)
                cur = st.selectbox("Currency", ["₹ (INR)","$ (USD)","€ (EUR)"])
            pr   = st.text_input("Products (comma separated)", value=", ".join(cfg.get("products",[])))
            tc1, tc2 = st.columns(2)
            with tc1: ts_val = st.number_input("Daily Sales Target (₹)", value=int(cfg.get("target_daily_sales",50000)), step=5000)
            with tc2: tp_val = st.number_input("Daily Prod. Target",      value=int(cfg.get("target_daily_production",200)), step=10)
            if st.form_submit_button("💾 Save", use_container_width=True):
                new_cfg = {**cfg,"business_name":bn,"owner_name":on,"city":ci,"business_type":bt,
                           "currency":cur.split(" ")[0],
                           "products":[p.strip() for p in pr.split(",") if p.strip()],
                           "target_daily_sales":ts_val,"target_daily_production":tp_val}
                save_config(new_cfg); st.session_state.config = new_cfg
                st.success("✅ Saved!"); st.rerun()

    with tab2:
        st.markdown('<div class="cbox"><strong style="color:#F97316;">OpenRouter API Key</strong><br><span style="color:#8B949E;font-size:.83rem;">Get free key at <a href="https://openrouter.ai" target="_blank" style="color:#3B82F6;">openrouter.ai</a>. Without key, smart mock AI is used.</span></div>', unsafe_allow_html=True)
        with st.form("ai_form"):
            ak = st.text_input("API Key", value=cfg.get("api_key",""), type="password", placeholder="sk-or-v1-...")
            am = st.selectbox("Model", ["openai/gpt-4o-mini","openai/gpt-4o","anthropic/claude-3-haiku","anthropic/claude-3.5-sonnet","google/gemini-flash-1.5"])
            if st.form_submit_button("💾 Save AI Settings", use_container_width=True):
                cfg["api_key"] = ak; cfg["ai_model"] = am
                save_config(cfg); st.session_state.config = cfg; st.success("✅ Saved!")
        if cfg.get("api_key",""):
            if st.button("🧪 Test Connection"):
                with st.spinner("Testing..."):
                    resp = call_ai("Say 'BizPilot AI v2.1 connected!'", "You are a helpful assistant.", cfg["api_key"])
                st.markdown(f'<div class="rbox">{resp}</div>', unsafe_allow_html=True)
        else:
            st.info("ℹ️ No API key — using mock AI. Add key for live responses.")

    with tab3:
        ops = load_ops(); crm = load_crm(); msgs = load_messages(); responses = load_responses()
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.markdown(f'<div class="mc bl"><div class="mc-val" style="font-size:1.3rem;">{len(ops)}</div><div class="mc-lbl">Ops Entries</div></div>', unsafe_allow_html=True)
        mc2.markdown(f'<div class="mc gr"><div class="mc-val" style="font-size:1.3rem;">{len(crm)}</div><div class="mc-lbl">CRM Leads</div></div>', unsafe_allow_html=True)
        mc3.markdown(f'<div class="mc or"><div class="mc-val" style="font-size:1.3rem;">{len(msgs)}</div><div class="mc-lbl">Messages</div></div>', unsafe_allow_html=True)
        mc4.markdown(f'<div class="mc pu"><div class="mc-val" style="font-size:1.3rem;">{len(responses)}</div><div class="mc-lbl">Responses</div></div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        d1, d2 = st.columns(2)
        with d1:
            st.download_button("📥 Export Ops", data=json.dumps(ops,indent=2,ensure_ascii=False), file_name="ops.json", mime="application/json")
        with d2:
            st.download_button("📥 Export CRM", data=json.dumps(crm,indent=2,ensure_ascii=False), file_name="crm.json", mime="application/json")
        if check_access(["Owner"]):
            with st.expander("⚠️ Danger Zone"):
                st.warning("Irreversible!")
                da, db = st.columns(2)
                with da:
                    if st.button("🗑️ Reset Ops"):  save_ops([]);  st.success("Done"); st.rerun()
                with db:
                    if st.button("🗑️ Reset CRM"):  save_crm([]); st.success("Done"); st.rerun()
        st.markdown('<div class="cbox" style="margin-top:1rem;"><strong style="color:#F97316;">📁 Data Location</strong><br><code style="color:#8B949E;">./bizpilot_data/</code><br><span style="color:#8B949E;font-size:.8rem;">All data stored locally. AI queries sent to OpenRouter only when API key is set.</span></div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# MAIN  ←── THE FIX: called unconditionally at module level, NOT inside
#            `if __name__ == "__main__":` which Streamlit never executes
# ═══════════════════════════════════════════════════════════════════════════
def main():
    # 1. Always inject CSS first — never let this crash the app
    inject_css()

    # 2. Always initialise session state — safe even on first run
    init_session()

    # 3. Decide: show login or the main app
    if not st.session_state.get("logged_in", False):
        page_login()
        return

    # 4. Render sidebar (safe wrapper)
    try:
        render_sidebar()
    except Exception as e:
        st.sidebar.error(f"Sidebar error: {e}")

    # 5. Route to the right page — default to Dashboard on any error
    page = st.session_state.get("page", "Dashboard")
    dispatch = {
        "Dashboard":    page_dashboard,
        "Daily Entry":  page_daily_entry,
        "Daily Ops":    page_daily_ops,
        "CRM":          page_crm,
        "Reports":      page_reports,
        "AI Assistant": page_ai_assistant,
        "Workforce":    page_workforce,
        "Scheduler":    page_scheduler,
        "Owner HQ":     page_owner_hq,
        "Activity":     page_activity,
        "Settings":     page_settings,
    }
    page_fn = dispatch.get(page, page_dashboard)
    try:
        page_fn()
    except Exception as e:
        st.error(f"⚠️ Page error on '{page}': {e}")
        st.info("Try selecting a different page from the sidebar, or refresh the browser.")


# ── CRITICAL FIX: call main() directly — Streamlit imports this module,
#    so `if __name__ == '__main__':` is NEVER True and main() is never called.
main()