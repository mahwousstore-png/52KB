"""
engines/ai_engine.py v19.0
- Gemini مباشر + Grounding (بحث حقيقي)
- fragranticarabia.com → صور + مكونات العطور
- Mahwous وصف خاص للمنتجات المفقودة
- تحقق منتج | بحث سوق | تحليل مجمع | دردشة
"""
import requests, json, re, time
from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY

_GM  = "gemini-2.0-flash"
_GU  = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:generateContent"
_GUS = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:streamGenerateContent"
_OR  = "https://openrouter.ai/api/v1/chat/completions"
_CO  = "https://api.cohere.ai/v1/generate"
_FR  = "https://www.fragranticarabia.com"

# ══ System Prompts مخصصة لكل قسم ══════════════
PAGE_PROMPTS = {
"price_raise": """أنت خبير تسعير عطور فاخرة (السوق السعودي) — قسم «سعر أعلى».
سعرنا أعلى من المنافس. قواعد: فرق<10 → إبقاء | 10-30 → مراجعة | >30 → خفض فوري.
لكل منتج: 1.هل المطابقة صحيحة؟ 2.هل الفرق مبرر؟ 3.السعر المقترح.
أجب بالعربية بإيجاز واحترافية.""",
"price_lower": """أنت خبير تسعير عطور فاخرة (السوق السعودي) — قسم «سعر أقل».
سعرنا أقل من المنافس = فرصة ربح ضائعة. فرق<10 → إبقاء | 10-50 → رفع تدريجي | >50 → رفع فوري.
لكل منتج: 1.هل يمكن رفع السعر؟ 2.السعر الأمثل. أجب بالعربية بإيجاز.""",
"approved": "أنت خبير تسعير عطور. راجع المنتجات الموافق عليها وتأكد من استمرار صلاحيتها. أجب بالعربية.",
"missing": """أنت خبير عطور فاخرة — متخصص في المنتجات المفقودة بمتجر مهووس.
لكل منتج: 1.هل هو حقيقي وموثوق؟ 2.هل يستحق الإضافة؟ 3.السعر المقترح. 4.أولوية الإضافة (عالية/متوسطة/منخفضة). أجب بالعربية.""",
"review": "أنت خبير تسعير عطور. قرّر: ✅موافق / 📉مخفض / 🔴أعلى / 🗑️إزالة. أجب بالعربية.",
"general": """أنت مساعد ذكاء اصطناعي متخصص في تسعير العطور الفاخرة والسوق السعودي.
خبرتك: تحليل الأسعار، المنافسة، استراتيجيات التسعير، مكونات العطور ومراكز الرائحة.
أجب بالعربية باحترافية وإيجاز — يمكنك استخدام الـ markdown.""",
"verify": """أنت خبير تحقق من منتجات العطور.
أجب JSON فقط: {"match":true/false,"confidence":0-100,"reason":"","suggestion":"","market_price":0}""",
"market_search": """أنت محلل أسعار عطور (السوق السعودي).
أجب JSON: {"market_price":0,"price_range":{"min":0,"max":0},"competitors":[{"name":"","price":0}],"recommendation":""}""",
}

# ══ استدعاء Gemini ══════════════════════════
def _call_gemini(prompt, system="", grounding=False, stream=False):
    full = f"{system}\n\n{prompt}" if system else prompt
    payload = {
        "contents": [{"parts": [{"text": full}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 4096, "topP": 0.85}
    }
    if grounding:
        payload["tools"] = [{"google_search": {}}]

    for key in GEMINI_API_KEYS:
        if not key: continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=35)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    parts = data["candidates"][0]["content"]["parts"]
                    return "".join(p.get("text","") for p in parts)
            elif r.status_code == 429:
                time.sleep(1); continue
        except: continue
    return None

def _call_openrouter(prompt, system=""):
    if not OPENROUTER_API_KEY: return None
    try:
        msgs = []
        if system: msgs.append({"role":"system","content":system})
        msgs.append({"role":"user","content":prompt})
        r = requests.post(_OR, json={
            "model":"google/gemini-2.0-flash-001",
            "messages":msgs,"temperature":0.3,"max_tokens":4096
        }, headers={"Authorization":f"Bearer {OPENROUTER_API_KEY}"}, timeout=35)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
    except: pass
    return None

def _call_cohere(prompt, system=""):
    if not COHERE_API_KEY: return None
    try:
        full = f"{system}\n\n{prompt}" if system else prompt
        r = requests.post(_CO, json={
            "model":"command-r-plus","prompt":full,"max_tokens":4096,"temperature":0.3
        }, headers={"Authorization":f"Bearer {COHERE_API_KEY}"}, timeout=35)
        if r.status_code == 200:
            return r.json().get("generations",[{}])[0].get("text","")
    except: pass
    return None

def call_ai(prompt, page="general"):
    sys = PAGE_PROMPTS.get(page, PAGE_PROMPTS["general"])
    for fn in [lambda: _call_gemini(prompt, sys),
               lambda: _call_openrouter(prompt, sys),
               lambda: _call_cohere(prompt, sys)]:
        r = fn()
        if r: return {"success":True,"response":r,"source":fn.__name__ if hasattr(fn,"__name__") else "AI"}
    # fallback source names
    for r, src in [(_call_gemini(prompt,sys),"Gemini"),
                   (_call_openrouter(prompt,sys),"OpenRouter"),
                   (_call_cohere(prompt,sys),"Cohere")]:
        if r: return {"success":True,"response":r,"source":src}
    return {"success":False,"response":"❌ فشل الاتصال بجميع مزودي AI","source":"none"}

# ══ Gemini Chat مع History ══════════════════
def gemini_chat(message, history=None, system_extra=""):
    """دردشة Gemini مع كامل تاريخ المحادثة"""
    sys = PAGE_PROMPTS["general"]
    if system_extra:
        sys = f"{sys}\n\nسياق إضافي: {system_extra}"

    contents = []
    for h in (history or [])[-12:]:
        contents.append({"role":"user","parts":[{"text":h["user"]}]})
        contents.append({"role":"model","parts":[{"text":h["ai"]}]})
    contents.append({"role":"user","parts":[{"text":f"{sys}\n\n{message}"}]})

    payload = {"contents":contents,
               "generationConfig":{"temperature":0.4,"maxOutputTokens":4096,"topP":0.9}}

    for key in GEMINI_API_KEYS:
        if not key: continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=40)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                    return {"success":True,"response":text,"source":"Gemini Flash"}
            elif r.status_code == 429:
                time.sleep(1); continue
        except: continue

    r = _call_openrouter(message, sys)
    if r: return {"success":True,"response":r,"source":"OpenRouter"}
    return {"success":False,"response":"❌ فشل الاتصال","source":"none"}

# ══ تحقق منتج ═══════════════════════════════
def verify_match(p1, p2, pr1=0, pr2=0):
    prompt = f"""تحقق من تطابق هذين المنتجين:
منتج 1: {p1} | السعر: {pr1:.0f} ر.س
منتج 2: {p2} | السعر: {pr2:.0f} ر.س
هل هما نفس العطر؟ (ماركة + اسم + حجم + نوع EDP/EDT)"""
    sys = PAGE_PROMPTS["verify"]
    txt = _call_gemini(prompt, sys) or _call_openrouter(prompt, sys)
    if not txt: return {"success":False,"match":False,"confidence":0,"reason":"فشل AI"}
    try:
        clean = re.sub(r'```json|```','',txt).strip()
        s=clean.find('{'); e=clean.rfind('}')+1
        data = json.loads(clean[s:e])
        return {"success":True, **data}
    except:
        return {"success":True,"match":"true" in txt.lower(),"confidence":70,"reason":txt[:200]}

# ══ بحث أسعار السوق ═════════════════════════
def search_market_price(product_name, our_price=0):
    prompt = (f"ما هو سعر السوق السعودي الحالي لـ: «{product_name}»؟\n"
              f"سعرنا الحالي: {our_price:.0f} ر.س\n"
              "اذكر: سعر السوق، نطاق الأسعار، أهم 3 منافسين وأسعارهم، توصيتك.")
    sys = PAGE_PROMPTS["market_search"]
    txt = _call_gemini(prompt, sys, grounding=True) or _call_gemini(prompt, sys) or _call_openrouter(prompt, sys)
    if not txt: return {"success":False,"market_price":0}
    try:
        clean = re.sub(r'```json|```','',txt).strip()
        s=clean.find('{'); e=clean.rfind('}')+1
        if s>=0 and e>s:
            return {"success":True, **json.loads(clean[s:e])}
    except: pass
    return {"success":True,"market_price":our_price,"recommendation":txt[:300]}

# ══ بحث صورة ومكونات من Fragrantica Arabia ══
def fetch_fragrantica_info(product_name):
    """
    يبحث عن صورة + مكونات العطر من Fragrantica Arabia
    يستخدم Gemini Grounding للوصول للموقع
    """
    prompt = f"""ابحث عن العطر «{product_name}» في موقع https://www.fragranticarabia.com

أحتاج:
1. رابط صورة المنتج (image URL)
2. مكونات العطر (Top notes, Middle notes, Base notes)
3. وصف قصير للعطر بالعربية
4. الماركة والنوع (EDP/EDT) والحجم

أجب JSON فقط:
{{
  "image_url": "رابط الصورة أو فارغ",
  "top_notes": ["مكون1","مكون2"],
  "middle_notes": ["مكون1","مكون2"],
  "base_notes": ["مكون1","مكون2"],
  "description_ar": "وصف قصير بالعربية",
  "brand": "",
  "type": "",
  "fragrantica_url": "رابط الصفحة"
}}"""

    txt = _call_gemini(prompt, grounding=True)
    if not txt:
        txt = _call_gemini(prompt)
    if not txt:
        return {"success":False}
    try:
        clean = re.sub(r'```json|```','',txt).strip()
        s=clean.find('{'); e=clean.rfind('}')+1
        if s>=0 and e>s:
            data = json.loads(clean[s:e])
            return {"success":True, **data}
    except: pass
    return {"success":False,"description_ar":txt[:200] if txt else ""}

# ══ وصف مهووس للمنتجات المفقودة ════════════
def generate_mahwous_description(product_name, price, fragrantica_data=None):
    """
    يولّد وصفاً بتنسيق مهووس الاحترافي:
    اسم العطر، الماركة، المكونات، الوصف الشعري، السعر المقترح
    """
    frag_info = ""
    if fragrantica_data and fragrantica_data.get("success"):
        top = ", ".join(fragrantica_data.get("top_notes",[])[:4])
        mid = ", ".join(fragrantica_data.get("middle_notes",[])[:4])
        base = ", ".join(fragrantica_data.get("base_notes",[])[:4])
        desc = fragrantica_data.get("description_ar","")
        frag_info = f"\nالمكونات - قمة: {top} | قلب: {mid} | قاعدة: {base}\nوصف: {desc}"

    prompt = f"""اكتب وصفاً احترافياً وجذاباً لهذا العطر بتنسيق متجر مهووس:

العطر: {product_name}
السعر: {price:.0f} ر.س{frag_info}

التنسيق المطلوب (اتبعه بدقة):
---
🌟 [اسم العطر الكامل]

✨ [جملة تسويقية جذابة بالعربية - سطر واحد]

📝 [فقرة وصف شعري 2-3 جمل تصف رائحة العطر وأجواءه]

🌸 مكونات العطر:
• القمة: [المكونات]
• القلب: [المكونات]
• القاعدة: [المكونات]

👤 مناسب لـ: [الجنس] | 🕐 المناسبة: [النهار/المساء/المختلطة]

💰 السعر: {price:.0f} ر.س
---
أجب بالعربية فقط."""

    txt = _call_gemini(prompt) or _call_openrouter(prompt) or _call_cohere(prompt)
    return txt or f"🌟 {product_name}\n💰 السعر: {price:.0f} ر.س"

# ══ بحث mahwous.com ══════════════════════════
def search_mahwous(product_name):
    prompt = f"""هل العطر «{product_name}» متوفر في متجر مهووس؟
أجب JSON: {{"likely_available":true/false,"confidence":0-100,
"similar_products":[],"add_recommendation":"عالية/متوسطة/منخفضة",
"reason":"سبب مختصر","suggested_price":0}}"""
    txt = _call_gemini(prompt, grounding=True) or _call_gemini(prompt)
    if not txt: return {"success":False}
    try:
        clean = re.sub(r'```json|```','',txt).strip()
        s=clean.find('{'); e=clean.rfind('}')+1
        if s>=0 and e>s:
            return {"success":True, **json.loads(clean[s:e])}
    except: pass
    return {"success":True,"likely_available":False,"confidence":50,"reason":txt[:150]}

# ══ تحقق مكرر ═══════════════════════════════
def check_duplicate(product_name, our_products):
    if not our_products:
        return {"success":True,"response":"لا توجد بيانات للمقارنة"}
    sample = our_products[:30]
    prompt = f"""هل العطر «{product_name}» موجود بشكل مشابه في هذه القائمة؟
القائمة: {', '.join(str(p) for p in sample)}
أجب: نعم (وذكر أقرب مطابقة) أو لا."""
    r = call_ai(prompt, "missing")
    return r

# ══ تحليل مجمع ══════════════════════════════
def bulk_verify(items, section="general"):
    if not items: return {"success":False,"response":"لا توجد منتجات"}
    lines = "\n".join(
        f"{i+1}. {it.get('our','')} ↔ {it.get('comp','')} | "
        f"سعرنا: {it.get('our_price',0):.0f} | منافس: {it.get('comp_price',0):.0f} | "
        f"فرق: {it.get('our_price',0)-it.get('comp_price',0):+.0f}"
        for i,it in enumerate(items)
    )
    prompt = f"حلّل هذه المنتجات وأعطِ توصية لكل منها:\n{lines}"
    return call_ai(prompt, section)

# ══ معالجة النص الملصوق ═════════════════════
def analyze_paste(text, context=""):
    """تحليل نص ملصوق من Excel أو أي مصدر"""
    prompt = f"""المستخدم لصق هذا النص:{chr(10) + context if context else ''}

---
{text[:5000]}
---

حلّل هذا النص واستخرج:
1. هل هو قائمة منتجات؟ إذا نعم، اعرضها بشكل منظم
2. إذا كانت أسعار، حللها وقارنها
3. إذا كانت أوامر، نفذها وأخبر بالنتيجة
4. أعطِ توصيات مفيدة بناءً على البيانات
أجب بالعربية بشكل منظم."""
    return call_ai(prompt, "general")

# ══ دوال متوافقة مع app.py القديم ══════════
def chat_with_ai(msg, history=None, ctx=""): return gemini_chat(msg, history, ctx)
def analyze_product(p, price=0): return call_ai(f"حلّل: {p} ({price:.0f}ر.س)", "general")
def suggest_price(p, comp_price): return call_ai(f"اقترح سعراً لـ {p} بدلاً من {comp_price:.0f}ر.س", "general")
def process_paste(text): return analyze_paste(text)
