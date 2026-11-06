"""Offline test harness. Does not contact Telegram, bank or Fragment."""
import sys, os, types, tempfile, importlib.util
from pathlib import Path

class MockTeleBot:
    def __init__(self, *a, **k):
        self.sent=[]
    def message_handler(self,*a,**k): return lambda fn:fn
    def callback_query_handler(self,*a,**k): return lambda fn:fn
    def pre_checkout_query_handler(self,*a,**k): return lambda fn:fn
    def send_message(self,uid,text,**kw): self.sent.append((uid,text));return True
    def get_my_star_balance(self):return types.SimpleNamespace(amount=10000)
    def send_gift(self,**kw):return True
    def refund_star_payment(self,**kw):return True
    def get_available_gifts(self):return types.SimpleNamespace(gifts=[])
    def get_me(self):return types.SimpleNamespace(username='testbot',id=2233)
    def set_my_commands(self,*a,**k):return True
    def set_webhook(self,*a,**k):return True

class MockFlask:
    def __init__(self,*a,**kw):self.config={}
    def get(self,*a,**kw):return lambda fn:fn
    def post(self,*a,**kw):return lambda fn:fn

modtele=types.ModuleType('telebot')
modtele.TeleBot=MockTeleBot
modtypes=types.ModuleType('telebot.types')
class MockMarkup:
    def __init__(self,*a,**k):self.buttons=[]
    def row(self,*a):self.buttons.append(a)
    def add(self,*a):self.buttons.append(a)
class MockButton:
    def __init__(self,*a,**k):self.args=a;self.kwargs=k
modtypes.InlineKeyboardMarkup=MockMarkup
modtypes.InlineKeyboardButton=MockButton
modtypes.CopyTextButton=MockButton
modtele.types=modtypes
sys.modules['telebot']=modtele
sys.modules['telebot.types']=modtypes
fl=types.ModuleType('flask')
fl.Flask=MockFlask
fl.abort=lambda x: None
fl.jsonify=lambda **kw:kw
fl.request=types.SimpleNamespace()
sys.modules['flask']=fl
os.environ['BOT_TOKEN']='123456:abcDEFGhiJK'
os.environ['OWNER_ID']='999'
os.environ.pop('PUBLIC_URL',None)
os.environ.pop('RENDER_EXTERNAL_URL',None)
os.environ['PERSISTENT_DB_REQUIRED']='0'
with tempfile.TemporaryDirectory() as d:
    os.environ['DB_PATH']=str(Path(d)/'test.sqlite3')
    spec=importlib.util.spec_from_file_location('premium_v4', str(Path(__file__).parent/'bot.py'))
    app=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    app.init_db()
    assert len(app.qall('select * from bundles'))>=9
    cost,sale=app.get_price('stars',50)
    assert sale==18888 and cost==0
    app.change("INSERT INTO users(uid,username,first_name,referrer,created) VALUES(100,'buyer','Test',200,1)")
    app.change("INSERT INTO users(uid,username,first_name,created) VALUES(200,'ref','Test',1)")
    import time
    now=int(time.time())
    def create_order(uid,status,amount=10):
        with app.db() as c:
            cur=c.execute('''INSERT INTO orders(uid,kind,units,recipient_username,cost_uzs,price_uzs,original_xtr,payable_xtr,created,status,payable_uzs)
                          VALUES(?, 'stars', 50,'friend',10000,18888,?,?,?, ?, 18888)''',
                          (uid,amount,amount,now,status))
            return cur.lastrowid
    oid=create_order(100,'invoice',18)
    assert app.record_verified_receipt(oid,100,'charge-first',18)=='paid'
    assert app.record_verified_receipt(oid,100,'charge-first',18)=='duplicate'
    assert app.q1('select status from orders where id=?',(oid,))['status']=='paid'
    # Delivery can only be done once. No REAL API call is made.
    app.api_call=lambda path, body=None,timeout=1:{'ok':True,'test':True}
    app.complete_order(oid)
    app.complete_order(oid)
    assert app.q1('select status from orders where id=?',(oid,))['status']=='done'
    assert app.user_row(200)['bonus_uzs']==944  # floor(18888 * 5%)
    expired=create_order(100,'expired',19)
    assert app.record_verified_receipt(expired,100,'late-charge',19)=='refund_pending'
    assert app.q1('select status from orders where id=?',(expired,))['status']=='expired'
    app.refund_unfulfilled_receipts()
    assert app.q1('select status from payment_receipts where charge_id=?',('late-charge',))['status']=='refunded'
    assert app.record_verified_receipt(expired,100,'late-charge',19)=='duplicate'
    # Duplicate checkout (separate charge) is automatically refunded, not redelivered.
    assert app.record_verified_receipt(oid,100,'charge-second',18)=='refund_pending'
    app.refund_unfulfilled_receipts()
    assert app.q1('select status from payment_receipts where charge_id=?',('charge-second',))['status']=='refunded'
    assert app.user_row(200)['bonus_uzs']==944
    assert app.parse_humo_notification("🎉 To'ldirish\n➕ 15.000,00 UZS\n💳 HUMOCARD *1382")==(15000,'1382')
    assert app.parse_humo_notification("💸 To'lov\n➖ 15.000,00 UZS\n💳 HUMOCARD *1382") is None
    app.topup_menu(100)
    assert 'XTR' in app.bot.sent[-1][1]
    print('PASS: DB, package UZS, paid receipt once, auto fulfillment once, referral once, late refund, HUMO audit, user checkout policy')
