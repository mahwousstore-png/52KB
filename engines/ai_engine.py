"""
engines/ai_engine.py v22.0
- Gemini مباشر + Grounding (بحث حقيقي)
- fragranticarabia.com → صور + مكونات العطور
- Mahwous وصف SEO منظم + قواعد تسعير صارمة
- تحقق منتج | بحث سوق (أرخص 5 منافسين) | تحليل مجمع | دردشة
"""
import requests, json, re, time
from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY

_GM  = "gemini-2.0-flash"
_GU  = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:generateContent"
_GUS = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:streamGenerateContent"
_OR  = "https://openrouter.ai/api/v1/chat/completions"
_CO  = "https://api.cohere.ai/v1/generate"
_FR  = "https://www.fragranticarabia.com"

# ══ System Prompts مخصصة لكل قسم — v22.0 بقواعد صارمة ══
PAGE_PROMPTS = {
"price_raise": """أنت خبير تسعير عطور فاخرة (السوق السعودي) — قسم «سعر أعلى».
سعرنا أعلى من المنافس.

⚠️ قواعد صارمة يجب الالتزام بها:
- يُمنع اقتراح تخفيض يزيد عن 15% من سعرنا الحالي لحماية هامش الربح.
- إذا كان الفرق أقل من 5 ريال → اقترح عدم التخفيض لحماية الأرباح.
- إذا كان الفرق 5-30 ريال → اقترح تخفيضاً يجعلنا بنفس سعر المنافس أو أقل بريال.
- إذا كان الفرق أكثر من 30 ريال → اقترح تخفيضاً فورياً لا يتجاوز 15% من سعرنا.

لكل منتج: 1.هل المطابقة صحيحة؟ 2.هل الفرق مبرر؟ 3.السعر المقترح الدقيق.
أجب بالعربية بإيجاز واحترافية.""",

"price_lower": """أنت خبير تسعير عطور فاخرة (السوق السعودي) — قسم «سعر أقل».
سعرنا أقل من المنافس = فرصة ربح ضائعة.

⚠️ قواعد صارمة يجب الالتزام بها:
- اقترح رفع السعر ليكون أرخص من المنافس بـ 2 إلى 5 ريالات فقط — لزيادة الربح مع البقاء الأرخص.
- لا تقترح سعراً مساوياً أو أعلى من المنافس.
- احسب الربح المتوقع من فرق السعر المقترح.

لكل منتج: 1.هل يمكن رفع السعر؟ 2.السعر الأمثل (المنافس - 2 إلى 5 ريال). 3.الربح المتوقع شهرياً.
أجب بالعربية بإيجاز.""",

"approved": "أنت خبير تسعير عطور. راجع المنتجات الموافق عليها وتأكد من استمرار صلاحيتها. أجب بالعربية.",

"missing": """أنت خبير عطور فاخرة — متخصص في المنتجات المفقودة بمتجر مهووس.
لكل منتج: 1.هل هو حقيقي وموثوق؟ 2.هل يستحق الإضافة؟ 3.السعر المقترح. 4.أولوية الإضافة (عالية/متوسطة/منخفضة). أجب بالعربية.""",

"review": """أنت خبير تحقق من تطابق منتجات العطور.

⚠️ قواعد صارمة:
- تجاهل السعر تماماً — السعر ليس معياراً للتطابق.
- قارن فقط: الحجم (ml) + التركيز (EDP/EDT/Extrait) + الإصدار (Intense/Noir/Sport).
- يجب أن يكون ردك حاسماً: إما متطابق 100% أو غير متطابق لسبب محدد.
- لا تقل "ربما" أو "يحتمل" — قرر بشكل قاطع.

لكل منتج: هل هما نفس العطر فعلاً؟ ✅ متطابق / ❌ غير متطابق — اذكر السبب المحدد.""",

"general": """أنت مساعد ذكاء اصطناعي متخصص في تسعير العطور الفاخرة والسوق السعودي.
خبرتك: تحليل الأسعار، المنافسة، استراتيجيات التسعير، مكونات العطور ومراكز الرائحة.
أجب بالعربية باحترافية وإيجاز — يمكنك استخدام الـ markdown.""",

"verify": """أنت خبير تحقق من منتجات العطور. تجاهل السعر — قارن الاسم والحجم والتركيز فقط.
أجب JSON فقط: {"match":true/false,"confidence":0-100,"reason":"سبب محدد","suggestion":"","market_price":0}""",

"market_search": """أنت محلل أسعار عطور (السوق السعودي). ابحث في الإنترنت عن أرخص 5 منافسين.
أجب JSON فقط:
{"market_price":0,"price_range":{"min":0,"max":0},
 "competitors":[{"name":"","price":0},{"name":"","price":0},{"name":"","price":0},{"name":"","price":0},{"name":"","price":0}],
 "recommendation":"توصية مختصرة بالعربية"}""",
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
        except (requests.RequestException, json.JSONDecodeError, KeyError):
            continue
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
    except (requests.RequestException, json.JSONDecodeError, KeyError, IndexError):
        pass
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
    except (requests.RequestException, json.JSONDecodeError, IndexError):
        pass
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
        except (requests.RequestException, json.JSONDecodeError, KeyError, IndexError):
            continue

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
    except (json.JSONDecodeError, ValueError):
        return {"success":True,"match":"true" in txt.lower(),"confidence":70,"reason":txt[:200]}

# ══ بحث أسعار السوق — أرخص 5 منافسين ══════
def search_market_price(product_name, our_price=0):
    """
    يبحث عن سعر السوق السعودي ويُعيد أرخص 5 منافسين.
    النتيجة: {"market_price":X, "price_range":{min,max},
              "competitors":[{"name":"","price":0}×5], "recommendation":""}
    """
    prompt = (
        f"ابحث في الإنترنت عن سعر العطر «{product_name}» في السوق السعودي الآن.\n"
        f"سعرنا الحالي: {our_price:.0f} ر.س\n\n"
        "أحتاج:\n"
        "1. متوسط سعر السوق\n"
        "2. أرخص 5 منافسين سعوديين (سلة، نون، ماكياج، الزاهد، نمشي، أمازون.sa، غيرها) مع أسعارهم الدقيقة\n"
        "3. توصية: هل سعرنا مناسب؟\n\n"
        "أجب JSON فقط بهذا الشكل بالضبط:\n"
        '{"market_price":0,"price_range":{"min":0,"max":0},'
        '"competitors":[{"name":"سلة","price":0},{"name":"نون","price":0},'
        '{"name":"ماكياج","price":0},{"name":"الزاهد","price":0},{"name":"نمشي","price":0}],'
        '"recommendation":"توصية مختصرة"}'
    )
    sys = PAGE_PROMPTS["market_search"]
    # Grounding أولاً للحصول على أسعار حقيقية
    txt = (_call_gemini(prompt, sys, grounding=True)
           or _call_gemini(prompt, sys)
           or _call_openrouter(prompt, sys))
    if not txt:
        return {"success": False, "market_price": 0, "competitors": []}
    try:
        clean = re.sub(r'```json|```', '', txt).strip()
        s = clean.find('{'); e = clean.rfind('}') + 1
        if s >= 0 and e > s:
            data = json.loads(clean[s:e])
            # تأكد من وجود قائمة المنافسين
            if "competitors" not in data:
                data["competitors"] = []
            # أرتّب المنافسين من الأرخص للأغلى
            data["competitors"] = sorted(
                [c for c in data["competitors"] if c.get("price", 0) > 0],
                key=lambda x: x.get("price", 9999)
            )[:5]
            return {"success": True, **data}
    except (json.JSONDecodeError, KeyError, Exception):
        pass
    return {"success": True, "market_price": our_price,
            "competitors": [], "recommendation": txt[:300]}

# ══ بحث صورة ومكونات من Fragrantica Arabia ══
def fetch_fragrantica_info(product_name):
    """
    يبحث عن صورة + مكونات العطر من Fragrantica Arabia.
    يُعيد image_url (رابط الصورة الرئيسية) + مكونات + وصف.
    يستخدم Gemini Grounding للوصول للموقع الحقيقي.
    """
    prompt = (
        f"ابحث عن العطر «{product_name}» في موقع fragranticarabia.com\n\n"
        "أحتاج بالضبط:\n"
        "1. رابط الصورة الرئيسية للعطر (image URL — يجب أن يبدأ بـ https://)\n"
        "2. مكونات العطر: النفحات العليا (Top)، القلب (Middle)، القاعدة (Base)\n"
        "3. وصف قصير جميل للعطر بالعربية (جملتين-ثلاث)\n"
        "4. الماركة + النوع (EDP/EDT/Extrait)\n"
        "5. رابط الصفحة على الموقع\n\n"
        "أجب JSON فقط:\n"
        '{"image_url":"https://...","top_notes":["مكون1","مكون2"],'
        '"middle_notes":["مكون1","مكون2"],"base_notes":["مكون1","مكون2"],'
        '"description_ar":"وصف جميل بالعربية",'
        '"brand":"","type":"EDP","fragrantica_url":"https://..."}'
    )
    # Grounding أولاً للحصول على الصورة الحقيقية
    txt = _call_gemini(prompt, grounding=True) or _call_gemini(prompt)
    if not txt:
        return {"success": False, "image_url": ""}
    try:
        clean = re.sub(r'```json|```', '', txt).strip()
        s = clean.find('{'); e = clean.rfind('}') + 1
        if s >= 0 and e > s:
            data = json.loads(clean[s:e])
            # تأكد image_url صالح
            img = data.get("image_url", "")
            if not img or not isinstance(img, str) or not img.startswith("http"):
                data["image_url"] = ""
            return {"success": True, **data}
    except (json.JSONDecodeError, KeyError, Exception):
        pass
    return {"success": False, "image_url": "", "description_ar": txt[:200] if txt else ""}

# ══ وصف مهووس SEO للمنتجات المفقودة ════════
def generate_mahwous_description(product_name, price, fragrantica_data=None):
    """
    يولّد وصف Markdown منظم بتنسيق SEO لمتجر مهووس.
    يشمل: فقرة افتتاحية + تفاصيل + رحلة العطر + FAQ + SEO metadata.
    """
    # استخراج بيانات Fragrantica إن توفرت
    brand = ""
    top_notes = middle_notes = base_notes = desc_ar = ""
    if fragrantica_data and fragrantica_data.get("success"):
        brand      = fragrantica_data.get("brand", "")
        top_notes  = ", ".join(fragrantica_data.get("top_notes",  [])[:5])
        middle_notes = ", ".join(fragrantica_data.get("middle_notes",[])[:5])
        base_notes = ", ".join(fragrantica_data.get("base_notes", [])[:5])
        desc_ar    = fragrantica_data.get("description_ar", "")

    frag_context = ""
    if brand or top_notes:
        frag_context = (
            f"\nبيانات العطر:\n"
            f"- الماركة: {brand}\n"
            f"- وصف: {desc_ar}\n"
            f"- النفحات العليا: {top_notes}\n"
            f"- نفحات القلب: {middle_notes}\n"
            f"- نفحات القاعدة: {base_notes}"
        )

    prompt = (
        f"اكتب وصف منتج احترافي SEO لعطر مهووس بالعربية.\n"
        f"العطر: {product_name}\n"
        f"السعر: {price:.0f} ر.س"
        f"{frag_context}\n\n"
        "اتبع هذا القالب بالضبط (Markdown):\n\n"
        "[فقرة افتتاحية جذابة 2-3 جمل تصف العطر وأجواءه]\n\n"
        "### تفاصيل المنتج\n"
        f"* **الماركة العالمية:** {brand or '[استنتج الماركة من الاسم]'}\n"
        "* **اسم العطر:** [اسم العطر فقط بدون الماركة]\n"
        "* **التركيز:** [EDP/EDT/Extrait]\n"
        "* **الحجم:** [Xml]\n"
        "* **الجنس:** [رجالي/نسائي/للجنسين]\n\n"
        "### رحلة العطر:\n"
        f"1. **النفحات العليا:** {top_notes or '[استنتج]'}\n"
        f"2. **نفحات القلب:** {middle_notes or '[استنتج]'}\n"
        f"3. **نفحات القاعدة:** {base_notes or '[استنتج]'}\n\n"
        "### متى تستخدمه؟\n"
        "* [مناسبة 1]\n"
        "* [مناسبة 2]\n\n"
        "### الأسئلة الشائعة\n"
        "* **هل المنتج أصلي؟** في مهووس جميع العطور أصلية ومضمونة 100%.\n"
        f"* **ما هو سعر {product_name}؟** متوفر بسعر {price:.0f} ر.س فقط.\n"
        "* **هل يوجد توصيل سريع؟** نعم، توصيل سريع لجميع مناطق المملكة.\n\n"
        "---\n"
        "**بيانات محركات البحث (SEO):**\n"
        f"* **Page Title:** [عنوان جذاب يتضمن اسم العطر والماركة — أقل من 60 حرف]\n"
        "* **Meta Description:** [وصف يتضمن الكلمات المفتاحية والسعر — أقل من 160 حرف]\n"
        "* **الكلمات المفتاحية:** [5 كلمات مفتاحية مفصولة بفاصلة]\n\n"
        "أجب بالعربية فقط. لا تضف أي نص قبل أو بعد القالب."
    )

    txt = _call_gemini(prompt) or _call_openrouter(prompt) or _call_cohere(prompt)
    if txt:
        return txt.strip()
    # Fallback بسيط إذا فشل AI
    return (
        f"{product_name} — عطر فاخر يأسرك من أول لحظة.\n\n"
        f"### تفاصيل المنتج\n"
        f"* **الماركة العالمية:** {brand or 'عالمية'}\n"
        f"* **اسم العطر:** {product_name}\n\n"
        f"### الأسئلة الشائعة\n"
        f"* **هل المنتج أصلي؟** في مهووس جميع العطور أصلية ومضمونة 100%.\n\n"
        f"---\n"
        f"**بيانات محركات البحث (SEO):**\n"
        f"* **Page Title:** {product_name} | مهووس للعطور\n"
        f"* **Meta Description:** اشترِ {product_name} بسعر {price:.0f} ر.س — أصلي ومضمون من مهووس."
    )

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
    except (json.JSONDecodeError, ValueError):
        pass
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
    _section_instructions = {
        "price_raise": "سعرنا أعلى. لكل منتج: 1.هل المطابقة صحيحة؟ 2.هل نخفض السعر؟ 3.السعر المقترح. رتّبهم من الأعلى فرقاً للأقل.",
        "price_lower": "سعرنا أقل = ربح ضائع. لكل منتج: 1.هل يمكن رفع السعر؟ 2.السعر الأمثل (أقل من المنافس بـ5-15). 3.الربح المتوقع.",
        "review": "هذه مطابقات غير مؤكدة. لكل منتج: هل هما نفس العطر فعلاً؟ ✅ نعم / ❌ لا / ⚠️ غير متأكد. اشرح السبب.",
        "approved": "هذه منتجات موافق عليها. راجعها وتأكد أنها لا تزال تنافسية.",
    }
    instruction = _section_instructions.get(section, "حلّل وأعطِ توصية لكل منتج.")
    prompt = f"{instruction}\n\nالمنتجات:\n{lines}"
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
