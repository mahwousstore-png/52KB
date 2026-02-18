"""
make_helper.py - أتمتة Make.com v20.0 (مُصلح)
═══════════════════════════════════════════════════════
الإصلاحات:
  1. تحديث الأسعار: يرسل product_id + price بالتنسيق الذي يتوقعه
     سيناريو "Integration Webhooks, Salla" (UpdateProduct)
  2. إضافة منتجات جديدة: يرسل "data" (وليس "products") بحقول عربية
     تطابق سيناريو "Mahwous - إضافة منتجات جديدة لسلة" (CreateProduct)
  3. إرسال دفعات (batches) لتجنب timeout عند إرسال أكثر من 50 منتج
  4. إعادة المحاولة (retry) مع تأخير تصاعدي
═══════════════════════════════════════════════════════
"""
import requests, json, time, math
from datetime import datetime
from config import WEBHOOK_UPDATE_PRICES, WEBHOOK_NEW_PRODUCTS


# ══════════════════════════════════════════════════════
#  ثوابت
# ══════════════════════════════════════════════════════
MAX_BATCH_SIZE = 50          # أقصى عدد منتجات في إرسال واحد
MAX_RETRIES    = 3           # عدد محاولات الإعادة
RETRY_DELAY    = 3           # ثواني بين المحاولات
TIMEOUT        = 30          # timeout بالثواني


# ══════════════════════════════════════════════════════
#  دالة إرسال أساسية مع retry
# ══════════════════════════════════════════════════════
def _post_with_retry(url, payload, timeout=TIMEOUT):
    """
    إرسال POST مع إعادة المحاولة عند الفشل.
    يرجع dict: {success, status_code, message, response_data}
    """
    last_error = ""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=timeout
            )
            if resp.status_code == 200:
                try:
                    resp_data = resp.json()
                except Exception:
                    resp_data = resp.text
                return {
                    "success": True,
                    "status_code": 200,
                    "message": "تم الإرسال بنجاح ✅",
                    "response_data": resp_data
                }
            elif resp.status_code == 429:
                # Rate limit — انتظر ثم أعد
                last_error = f"Rate limit (429) — محاولة {attempt+1}/{MAX_RETRIES}"
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            elif resp.status_code == 408:
                # Timeout
                last_error = f"Timeout (408) — محاولة {attempt+1}/{MAX_RETRIES}"
                time.sleep(RETRY_DELAY)
                continue
            else:
                return {
                    "success": False,
                    "status_code": resp.status_code,
                    "message": f"خطأ HTTP {resp.status_code}: {resp.text[:200]}",
                    "response_data": None
                }
        except requests.exceptions.Timeout:
            last_error = f"Connection timeout — محاولة {attempt+1}/{MAX_RETRIES}"
            time.sleep(RETRY_DELAY)
            continue
        except Exception as e:
            return {
                "success": False,
                "status_code": 0,
                "message": f"خطأ: {str(e)}",
                "response_data": None
            }

    return {
        "success": False,
        "status_code": 0,
        "message": f"فشل بعد {MAX_RETRIES} محاولات: {last_error}",
        "response_data": None
    }


# ══════════════════════════════════════════════════════
#  1. تحديث الأسعار → سيناريو "Integration Webhooks, Salla"
# ══════════════════════════════════════════════════════
#
#  ما يتوقعه Make.com (من Blueprint):
#  ─────────────────────────────────────
#  Webhook interface:
#    { "products": [ { "product_id": "text", "name": "text",
#                       "price": number, "sale_price": number,
#                       "quantity": number } ] }
#
#  Iterator: {{2.products}}
#  Salla UpdateProduct:
#    id    = {{4.product_id}}   ← معرّف المنتج في سلة (مطلوب!)
#    price = {{4.price}}        ← السعر الجديد
#
#  ⚠️ بدون product_id لن يعمل التحديث في سلة!
#  ─────────────────────────────────────

def send_price_updates(products, webhook_url=None):
    """
    إرسال تحديثات الأسعار إلى Make.com → سلة.

    كل عنصر في products يجب أن يحتوي على:
      - product_id  : معرّف المنتج في سلة (رقم أو نص)
      - price       : السعر الجديد
      - name        : اسم المنتج (اختياري — للتوثيق)
      - sale_price  : سعر التخفيض (اختياري)
      - quantity    : الكمية (اختياري)
    """
    url = webhook_url or WEBHOOK_UPDATE_PRICES
    if not products:
        return {"success": False, "status_code": 0,
                "message": "لا توجد منتجات للإرسال"}

    # ─── التحقق من وجود product_id ───
    missing_ids = [p for p in products if not p.get("product_id")]
    if missing_ids:
        names = [p.get("name", p.get("product_name", "؟")) for p in missing_ids[:5]]
        return {
            "success": False,
            "status_code": 0,
            "message": (
                f"⚠️ {len(missing_ids)} منتج بدون product_id! "
                f"سلة تحتاج معرّف المنتج لتحديث السعر.\n"
                f"أمثلة: {', '.join(names)}\n"
                f"💡 تأكد أن ملف منتجاتك يحتوي عمود معرّف المنتج (product_id أو id أو معرف)"
            )
        }

    # ─── إرسال بدفعات ───
    total_sent = 0
    total_failed = 0
    errors = []

    batches = _split_batches(products, MAX_BATCH_SIZE)
    for batch_num, batch in enumerate(batches, 1):
        # تنسيق يطابق Blueprint بالضبط
        payload = {
            "products": [
                {
                    "product_id": str(p["product_id"]),
                    "name": str(p.get("name", p.get("product_name", ""))),
                    "price": float(p.get("price", p.get("new_price", 0))),
                    "sale_price": float(p.get("sale_price", 0)) if p.get("sale_price") else None,
                    "quantity": int(p.get("quantity", 0)) if p.get("quantity") else None,
                }
                for p in batch
            ]
        }
        # إزالة القيم None
        for prod in payload["products"]:
            payload["products"] = [
                {k: v for k, v in prod.items() if v is not None}
                for prod in payload["products"]
            ]

        result = _post_with_retry(url, payload)
        if result["success"]:
            total_sent += len(batch)
        else:
            total_failed += len(batch)
            errors.append(f"دفعة {batch_num}: {result['message']}")

        # تأخير بين الدفعات
        if batch_num < len(batches):
            time.sleep(1)

    # ─── النتيجة ───
    if total_failed == 0:
        return {
            "success": True,
            "status_code": 200,
            "message": f"✅ تم إرسال {total_sent} منتج لتحديث الأسعار بنجاح"
        }
    elif total_sent > 0:
        return {
            "success": True,
            "status_code": 200,
            "message": (
                f"⚠️ تم إرسال {total_sent} منتج بنجاح، "
                f"فشل {total_failed} منتج.\n" +
                "\n".join(errors[:3])
            )
        }
    else:
        return {
            "success": False,
            "status_code": 0,
            "message": f"❌ فشل إرسال جميع المنتجات ({total_failed}).\n" + "\n".join(errors[:3])
        }


# ══════════════════════════════════════════════════════
#  2. إضافة منتجات جديدة → سيناريو "Mahwous - إضافة منتجات جديدة لسلة"
# ══════════════════════════════════════════════════════
#
#  ما يتوقعه Make.com (من Blueprint):
#  ─────────────────────────────────────
#  Webhook interface:
#    { "data": [ { "product_id": "text",
#                   "أسم المنتج": "text",
#                   "سعر المنتج": number,
#                   "رمز المنتج sku": "text",
#                   "الوزن": number,
#                   "سعر التكلفة": number,
#                   "السعر المخفض": number,
#                   "الوصف": "text" } ] }
#
#  Iterator: {{1.data}}
#  Salla CreateProduct:
#    name        = {{2.`أسم المنتج`}}
#    price       = {{2.`سعر المنتج`}}
#    sku         = {{2.`رمز المنتج sku`}}
#    weight      = {{2.`الوزن`}}
#    cost_price  = {{2.`سعر التكلفة`}}
#    sale_price  = {{2.`السعر المخفض`}}
#    description = {{2.`الوصف`}}
#    quantity    = "100"          (ثابت في Blueprint)
#    categories  = [قائمة ثابتة]  (ثابت في Blueprint)
#    product_type = "product"     (ثابت في Blueprint)
#  ─────────────────────────────────────

def send_new_products(products, webhook_url=None):
    """
    إرسال منتجات جديدة إلى Make.com → سلة.

    كل عنصر في products يجب أن يحتوي على:
      - أسم المنتج أو product_name : اسم المنتج
      - سعر المنتج أو price        : السعر
      - الوصف أو description        : وصف المنتج (اختياري)
      - رمز المنتج sku أو sku       : رمز SKU (اختياري)
      - الوزن أو weight             : الوزن (اختياري)
      - سعر التكلفة أو cost_price   : سعر التكلفة (اختياري)
      - السعر المخفض أو sale_price  : سعر التخفيض (اختياري)
    """
    url = webhook_url or WEBHOOK_NEW_PRODUCTS
    if not products:
        return {"success": False, "status_code": 0,
                "message": "لا توجد منتجات للإرسال"}

    # ─── إرسال بدفعات ───
    total_sent = 0
    total_failed = 0
    errors = []

    batches = _split_batches(products, MAX_BATCH_SIZE)
    for batch_num, batch in enumerate(batches, 1):
        # تنسيق يطابق Blueprint بالضبط — المفتاح "data" وليس "products"
        payload = {
            "data": [
                _format_new_product(p)
                for p in batch
            ]
        }

        result = _post_with_retry(url, payload)
        if result["success"]:
            total_sent += len(batch)
        else:
            total_failed += len(batch)
            errors.append(f"دفعة {batch_num}: {result['message']}")

        if batch_num < len(batches):
            time.sleep(1)

    # ─── النتيجة ───
    if total_failed == 0:
        return {
            "success": True,
            "status_code": 200,
            "message": f"✅ تم إرسال {total_sent} منتج جديد لسلة بنجاح"
        }
    elif total_sent > 0:
        return {
            "success": True,
            "status_code": 200,
            "message": (
                f"⚠️ تم إرسال {total_sent} منتج بنجاح، "
                f"فشل {total_failed} منتج.\n" +
                "\n".join(errors[:3])
            )
        }
    else:
        return {
            "success": False,
            "status_code": 0,
            "message": f"❌ فشل إرسال جميع المنتجات ({total_failed}).\n" + "\n".join(errors[:3])
        }


def _format_new_product(p):
    """
    تحويل dict المنتج إلى التنسيق العربي الذي يتوقعه Blueprint سلة.
    يقبل المفاتيح بالعربي أو الإنجليزي ويوحّدها.
    """
    return {
        "أسم المنتج": str(
            p.get("أسم المنتج",
            p.get("اسم المنتج",
            p.get("product_name",
            p.get("name", ""))))
        ),
        "سعر المنتج": float(
            p.get("سعر المنتج",
            p.get("price",
            p.get("السعر", 0))) or 0
        ),
        "رمز المنتج sku": str(
            p.get("رمز المنتج sku",
            p.get("sku",
            p.get("رمز_المنتج", "")))
        ),
        "الوزن": float(
            p.get("الوزن",
            p.get("weight", 0)) or 0
        ),
        "سعر التكلفة": float(
            p.get("سعر التكلفة",
            p.get("cost_price",
            p.get("سعر_التكلفة", 0))) or 0
        ),
        "السعر المخفض": float(
            p.get("السعر المخفض",
            p.get("sale_price",
            p.get("السعر_المخفض", 0))) or 0
        ),
        "الوصف": str(
            p.get("الوصف",
            p.get("description", ""))
        ),
    }


# ══════════════════════════════════════════════════════
#  3. إرسال منتجات مفقودة (تُضاف كمنتجات جديدة في سلة)
# ══════════════════════════════════════════════════════

def send_missing_products(products, webhook_url=None):
    """
    إرسال المنتجات المفقودة كمنتجات جديدة إلى سلة.
    يستخدم نفس سيناريو إضافة المنتجات الجديدة.
    """
    # المنتجات المفقودة تُرسل بنفس تنسيق المنتجات الجديدة
    return send_new_products(products, webhook_url)


# ══════════════════════════════════════════════════════
#  4. دوال مساعدة
# ══════════════════════════════════════════════════════

def _split_batches(items, batch_size):
    """تقسيم قائمة إلى دفعات"""
    if not items:
        return []
    return [items[i:i+batch_size] for i in range(0, len(items), batch_size)]


def send_to_make(data, webhook_type="update"):
    """دالة عامة للإرسال إلى Make"""
    if webhook_type == "update":
        return send_price_updates(data)
    elif webhook_type == "new":
        return send_new_products(data)
    elif webhook_type == "missing":
        return send_missing_products(data)
    return {"success": False, "message": "نوع غير معروف"}


def send_single_product(product, action="update"):
    """إرسال منتج واحد إلى Make"""
    return send_to_make([product], action)


def test_webhook(webhook_type="update"):
    """اختبار اتصال Webhook"""
    url = WEBHOOK_UPDATE_PRICES if webhook_type == "update" else WEBHOOK_NEW_PRODUCTS
    try:
        # إرسال بيانات اختبار بالتنسيق الصحيح
        if webhook_type == "update":
            payload = {
                "products": [{
                    "product_id": "TEST_000",
                    "name": "اختبار اتصال",
                    "price": 0
                }]
            }
        else:
            payload = {
                "data": [{
                    "أسم المنتج": "اختبار اتصال",
                    "سعر المنتج": 0,
                    "رمز المنتج sku": "TEST_000",
                    "الوزن": 0,
                    "سعر التكلفة": 0,
                    "السعر المخفض": 0,
                    "الوصف": "اختبار اتصال — يمكن تجاهله"
                }]
            }
        resp = requests.post(url, json=payload, timeout=15)
        return {
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "url": url,
            "message": "الاتصال ناجح ✅" if resp.status_code == 200
                       else f"فشل الاتصال: {resp.status_code}"
        }
    except Exception as e:
        return {"success": False, "url": url, "message": f"خطأ: {str(e)}"}


def verify_webhook_connection():
    """التحقق من جميع الاتصالات"""
    results = {
        "update_prices": test_webhook("update"),
        "new_products": test_webhook("new")
    }
    results["all_connected"] = all(
        r["success"] for k, r in results.items() if k != "all_connected"
    )
    return results


# ══════════════════════════════════════════════════════
#  5. تحويل DataFrame → تنسيق Make.com/سلة
# ══════════════════════════════════════════════════════

def export_to_make_format(df, section_type="update"):
    """
    تحويل DataFrame إلى صيغة تطابق سيناريوهات Make.com بالضبط.

    section_type:
      "update"  → تحديث أسعار (يحتاج product_id)
      "missing" → منتجات مفقودة (تُضاف كجديدة)
      "new"     → منتجات جديدة (price_lower)
    """
    from engines.engine import extract_brand, extract_size, classify_product

    products = []
    for _, row in df.iterrows():

        # ─── تحديث أسعار ───
        if section_type == "update":
            # استخراج product_id من أي عمود ممكن
            product_id = (
                row.get("معرف_المنتج", "") or
                row.get("معرف المنتج", "") or
                row.get("product_id", "") or
                row.get("id", "") or
                row.get("معرف", "") or
                row.get("ID", "") or
                ""
            )
            our_price = float(row.get("السعر", 0) or 0)
            comp_price = float(row.get("سعر_المنافس", 0) or 0)

            # السعر الجديد = سعر المنافس - 1 (أقل من المنافس بريال)
            new_price = comp_price - 1 if comp_price > 0 else our_price

            product = {
                "product_id": str(product_id),
                "name": str(row.get("المنتج", "")),
                "price": round(new_price, 2),
                "sale_price": None,
                "quantity": None,
                # ─── حقول إضافية للتوثيق (لا يستخدمها Blueprint لكن مفيدة للسجل) ───
                "_old_price": our_price,
                "_competitor_price": comp_price,
                "_competitor_name": str(row.get("المنافس", "")).replace('.csv','').replace('.xlsx',''),
                "_confidence": int(float(row.get("نسبة_التطابق", 0) or 0)),
                "_reason": str(row.get("القرار", "")),
            }
            products.append(product)

        # ─── منتجات مفقودة (تُضاف كجديدة) ───
        elif section_type == "missing":
            pname = str(row.get("منتج_المنافس", ""))
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            brand = str(row.get("الماركة", "")) or extract_brand(pname)
            size_val = extract_size(pname)
            size_str = f"{int(size_val)}ml" if size_val else str(row.get("الحجم", ""))

            # بناء وصف احترافي
            desc_parts = [f"عطر {brand}" if brand else "عطر"]
            if size_str:
                desc_parts.append(size_str)
            ptype = classify_product(pname)
            if ptype == "tester":
                desc_parts.append("تستر")
            description = " ".join(desc_parts)

            # بناء SKU من اسم المنتج
            sku = _generate_sku(pname, brand)

            product = {
                "أسم المنتج": pname,
                "سعر المنتج": round(comp_price - 1, 2) if comp_price > 0 else 0,
                "رمز المنتج sku": sku,
                "الوزن": _estimate_weight(size_val),
                "سعر التكلفة": round(comp_price * 0.6, 2) if comp_price > 0 else 0,
                "السعر المخفض": 0,
                "الوصف": description,
            }
            products.append(product)

        # ─── منتجات جديدة (من قسم سعر أقل) ───
        else:
            pname = str(row.get("المنتج", row.get("منتج_المنافس", "")))
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            our_price = float(row.get("السعر", 0) or 0)
            brand = str(row.get("الماركة", "")) or extract_brand(pname)
            size_val = extract_size(pname)
            size_str = f"{int(size_val)}ml" if size_val else str(row.get("الحجم", ""))

            desc_parts = [f"عطر {brand}" if brand else "عطر"]
            if size_str:
                desc_parts.append(size_str)
            description = " ".join(desc_parts)

            sku = _generate_sku(pname, brand)

            product = {
                "أسم المنتج": pname,
                "سعر المنتج": round(our_price if our_price > 0 else (comp_price - 1), 2),
                "رمز المنتج sku": sku,
                "الوزن": _estimate_weight(size_val),
                "سعر التكلفة": round(comp_price * 0.6, 2) if comp_price > 0 else 0,
                "السعر المخفض": 0,
                "الوصف": description,
            }
            products.append(product)

    return products


def _generate_sku(product_name, brand=""):
    """توليد SKU بسيط من اسم المنتج"""
    import re, hashlib
    # أخذ أول 3 حروف من الماركة + hash قصير
    brand_code = brand[:3].upper() if brand else "PRF"
    name_hash = hashlib.md5(product_name.encode('utf-8')).hexdigest()[:6].upper()
    return f"{brand_code}-{name_hash}"


def _estimate_weight(size_ml):
    """تقدير الوزن بالجرام من الحجم بالمل"""
    if not size_ml or size_ml <= 0:
        return 300  # وزن افتراضي
    # تقريب: وزن العبوة + السائل
    return int(size_ml * 1.2 + 150)
