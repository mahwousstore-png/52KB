"""
make_helper.py - أتمتة Make.com v22.0
═══════════════════════════════════════════════════════
الجديد في v22:
  1. UTF-8 صارم في جميع الإرساليات (العربية لا تنكسر في Make/سلة)
  2. SKU عشوائي MAH-XXXXXX للمنتجات الجديدة/المفقودة
  3. image_url في حمولة المنتجات الجديدة
  4. الوزن التلقائي: (size_ml × 1.2) + 150
  5. سعر التكلفة التلقائي: price × 0.6
═══════════════════════════════════════════════════════
"""
import requests, json, time, math, uuid
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
    إرسال POST مع UTF-8 صارم وإعادة المحاولة عند الفشل.
    يُشفّر JSON يدوياً بـ ensure_ascii=False لحماية النص العربي في Make/سلة.
    يرجع dict: {success, status_code, message, response_data}
    """
    last_error = ""
    for attempt in range(MAX_RETRIES):
        try:
            # UTF-8 صارم — العربية لا تُحوَّل إلى \uXXXX
            encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            resp = requests.post(
                url,
                data=encoded_payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
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
                last_error = f"Rate limit (429) — محاولة {attempt+1}/{MAX_RETRIES}"
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            elif resp.status_code == 408:
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
#    id    = {{4.product_id}}   ← معرّف المنتج في سلة
#    price = {{4.price}}        ← السعر الجديد
#
#  ⚠️ v21: إذا لم يوجد product_id → يُرسل مع تنبيه (لا يمنع الإرسال)
#  ─────────────────────────────────────

def send_price_updates(products, webhook_url=None):
    """
    إرسال تحديثات الأسعار إلى Make.com → سلة.

    كل عنصر في products يجب أن يحتوي على:
      - product_id  : معرّف المنتج في سلة (رقم أو نص) — مطلوب للتحديث
      - price       : السعر الجديد
      - name        : اسم المنتج (اختياري — للتوثيق والبحث)
      - sale_price  : سعر التخفيض (اختياري)
      - quantity    : الكمية (اختياري)
    
    v21: لا يمنع الإرسال إذا لم يوجد product_id — يُنبّه فقط
    """
    url = webhook_url or WEBHOOK_UPDATE_PRICES
    if not url or not url.startswith("http"):
        return {"success": False, "status_code": 0,
                "message": "❌ رابط Webhook غير صالح — تأكد من إعدادات Make.com"}
    if not products:
        return {"success": False, "status_code": 0,
                "message": "لا توجد منتجات للإرسال"}

    # ─── فصل المنتجات: مع معرّف / بدون معرّف ───
    with_id = []
    without_id = []
    for p in products:
        pid = _extract_product_id(p)
        if pid:
            p["_resolved_product_id"] = pid
            with_id.append(p)
        else:
            without_id.append(p)

    warnings = []
    if without_id:
        names = [p.get("name", p.get("المنتج", p.get("product_name", "؟")))
                 for p in without_id[:5]]
        warnings.append(
            f"⚠️ {len(without_id)} منتج بدون معرّف سلة (product_id).\n"
            f"أمثلة: {', '.join(names)}\n"
            f"💡 هذه المنتجات ستُرسل بالاسم فقط — تأكد أن سيناريو Make.com يدعم البحث بالاسم."
        )

    # ─── إرسال المنتجات التي لها معرّف ───
    all_products = with_id + without_id  # نرسل الكل — Make.com يتعامل مع الباقي
    
    total_sent = 0
    total_failed = 0
    errors = []

    batches = _split_batches(all_products, MAX_BATCH_SIZE)
    for batch_num, batch in enumerate(batches, 1):
        payload = {
            "products": [
                {
                    "product_id": str(p.get("_resolved_product_id", "")),
                    "name": str(p.get("name", p.get("المنتج", p.get("product_name", "")))),
                    "price": float(p.get("price", p.get("new_price", 0))),
                    **(_optional_field("sale_price", p.get("sale_price"))),
                    **(_optional_field("quantity", p.get("quantity"))),
                }
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
    msg_parts = []
    if total_sent > 0:
        msg_parts.append(f"✅ تم إرسال {total_sent} منتج لتحديث الأسعار")
        if len(with_id) > 0:
            msg_parts.append(f"({len(with_id)} بمعرّف سلة)")
        if len(without_id) > 0:
            msg_parts.append(f"({len(without_id)} بالاسم فقط)")
    if total_failed > 0:
        msg_parts.append(f"❌ فشل {total_failed} منتج")
        msg_parts.extend(errors[:3])
    if warnings:
        msg_parts.extend(warnings)

    return {
        "success": total_sent > 0,
        "status_code": 200 if total_sent > 0 else 0,
        "message": "\n".join(msg_parts),
        "stats": {
            "total": len(products),
            "with_id": len(with_id),
            "without_id": len(without_id),
            "sent": total_sent,
            "failed": total_failed,
        }
    }


def _extract_product_id(product_dict):
    """
    استخراج product_id من dict المنتج — يبحث في كل الأسماء الممكنة.
    يرجع القيمة أو "" إذا لم يوجد.
    """
    # ترتيب الأولوية: الأكثر تحديداً أولاً
    id_keys = [
        # مفاتيح مباشرة
        "product_id", "_resolved_product_id",
        # أسماء سلة العربية
        "معرف_المنتج", "معرف المنتج", "رقم المنتج", "رقم_المنتج",
        "المعرف", "معرف",
        # أسماء إنجليزية
        "Product ID", "Product_ID", "ID", "id", "Id",
        "NO", "No", "no", "NUMBER", "number", "Num", "num",
        # SKU
        "SKU", "sku", "Sku", "رمز المنتج", "رمز_المنتج", "رمز المنتج sku",
        # أسماء أخرى
        "الكود", "كود", "Code", "code",
        "الرقم", "رقم",
        "Barcode", "barcode", "الباركود",
    ]
    for key in id_keys:
        val = product_dict.get(key, "")
        if val and str(val) not in ("", "nan", "None", "0"):
            return str(val).strip()
    return ""


def _optional_field(key, value):
    """إرجاع dict بالحقل فقط إذا كانت القيمة موجودة وغير صفرية"""
    if value is not None and value != 0 and value != "":
        try:
            return {key: float(value)}
        except (ValueError, TypeError):
            return {}
    return {}


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
    if not url or not url.startswith("http"):
        return {"success": False, "status_code": 0,
                "message": "❌ رابط Webhook غير صالح — تأكد من إعدادات Make.com"}
    if not products:
        return {"success": False, "status_code": 0,
                "message": "لا توجد منتجات للإرسال"}

    total_sent = 0
    total_failed = 0
    errors = []

    batches = _split_batches(products, MAX_BATCH_SIZE)
    for batch_num, batch in enumerate(batches, 1):
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
    v21.1: يولّد SKU والوزن وسعر التكلفة تلقائياً إذا لم تُحدد.
    """
    name = str(
        p.get("أسم المنتج",
        p.get("اسم المنتج",
        p.get("product_name",
        p.get("name", ""))))
    )
    price = float(
        p.get("سعر المنتج",
        p.get("price",
        p.get("السعر", 0))) or 0
    )

    # SKU — توليد عشوائي فريد MAH-XXXXXX إذا لم يوجد
    sku = str(
        p.get("رمز المنتج sku",
        p.get("sku",
        p.get("رمز_المنتج", "")))
    )
    if not sku or sku in ("", "nan", "None"):
        sku = f"MAH-{uuid.uuid4().hex[:6].upper()}"

    # الوزن — تقدير تلقائي من الحجم إذا لم يُحدد
    weight = float(p.get("الوزن", p.get("weight", 0)) or 0)
    if weight <= 0:
        try:
            from engines.engine import extract_size
            size_ml = extract_size(name)
            weight = _estimate_weight(size_ml)
        except Exception:
            weight = 300  # وزن افتراضي

    # سعر التكلفة — تقدير 60% من السعر إذا لم يُحدد
    cost = float(
        p.get("سعر التكلفة",
        p.get("cost_price",
        p.get("سعر_التكلفة", 0))) or 0
    )
    if cost <= 0 and price > 0:
        cost = round(price * 0.6, 2)

    return {
        "أسم المنتج":     name,
        "سعر المنتج":     price,
        "رمز المنتج sku": sku,
        "الوزن":          weight,
        "سعر التكلفة":    cost,
        "السعر المخفض":   float(
            p.get("السعر المخفض",
            p.get("sale_price",
            p.get("السعر_المخفض", 0))) or 0
        ),
        "الوصف":    str(p.get("الوصف", p.get("description", ""))),
        "image_url": str(p.get("image_url", p.get("صورة", ""))),
    }


# ══════════════════════════════════════════════════════
#  3. إرسال منتجات مفقودة (تُضاف كمنتجات جديدة في سلة)
# ══════════════════════════════════════════════════════

def send_missing_products(products, webhook_url=None):
    """
    إرسال المنتجات المفقودة كمنتجات جديدة إلى سلة.
    يستخدم نفس سيناريو إضافة المنتجات الجديدة.
    """
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
      "update"  → تحديث أسعار (يستخدم product_id إن وُجد)
      "missing" → منتجات مفقودة (تُضاف كجديدة)
      "new"     → منتجات جديدة (price_lower)
    """
    from engines.engine import extract_brand, extract_size, classify_product

    products = []
    for _, row in df.iterrows():

        # ─── تحديث أسعار ───
        if section_type == "update":
            # استخراج product_id من كل الأعمدة الممكنة
            product_id = _extract_product_id_from_row(row)
            
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
                # حقول إضافية للتوثيق
                "_old_price": our_price,
                "_competitor_price": comp_price,
                "_competitor_name": str(row.get("المنافس", "")).replace('.csv','').replace('.xlsx',''),
                "_confidence": int(float(row.get("نسبة_التطابق", 0) or 0)),
                "_reason": str(row.get("القرار", "")),
            }
            products.append(product)

        # ─── منتجات مفقودة (تُضاف كجديدة في سلة) ───
        elif section_type == "missing":
            pname      = str(row.get("منتج_المنافس", ""))
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            brand      = str(row.get("الماركة", "")) or extract_brand(pname)
            size_val   = extract_size(pname) or 0
            size_str   = f"{int(size_val)}ml" if size_val else str(row.get("الحجم", ""))
            image_url  = str(row.get("image_url", row.get("صورة", "")))

            # وصف أساسي من البيانات المتوفرة
            desc_parts = [f"عطر {brand}" if brand else "عطر"]
            if size_str: desc_parts.append(size_str)
            ptype = classify_product(pname)
            if ptype == "tester": desc_parts.append("تستر")
            description = str(row.get("الوصف", "")) or " ".join(desc_parts)

            # SKU عشوائي فريد بصيغة MAH-XXXXXX
            sku = f"MAH-{uuid.uuid4().hex[:6].upper()}"

            # سعر البيع = سعر المنافس - 1 | التكلفة = 60% | الوزن = size×1.2+150
            sale_price = round(comp_price - 1, 2) if comp_price > 0 else 0
            cost_price = round(comp_price * 0.6, 2) if comp_price > 0 else 0
            weight     = int(size_val * 1.2 + 150) if size_val > 0 else 300

            product = {
                "أسم المنتج":      pname,
                "سعر المنتج":      sale_price,
                "رمز المنتج sku":  sku,
                "الوزن":           weight,
                "سعر التكلفة":     cost_price,
                "السعر المخفض":    0,
                "الوصف":           description,
                "image_url":       image_url,
            }
            products.append(product)

        # ─── منتجات جديدة (من قسم سعر أقل) ───
        else:
            pname      = str(row.get("المنتج", row.get("منتج_المنافس", "")))
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            our_price  = float(row.get("السعر", 0) or 0)
            brand      = str(row.get("الماركة", "")) or extract_brand(pname)
            size_val   = extract_size(pname) or 0
            size_str   = f"{int(size_val)}ml" if size_val else str(row.get("الحجم", ""))
            image_url  = str(row.get("image_url", row.get("صورة", "")))

            desc_parts = [f"عطر {brand}" if brand else "عطر"]
            if size_str: desc_parts.append(size_str)
            description = str(row.get("الوصف", "")) or " ".join(desc_parts)

            # SKU عشوائي فريد
            sku = f"MAH-{uuid.uuid4().hex[:6].upper()}"

            cost_price = round(comp_price * 0.6, 2) if comp_price > 0 else round(our_price * 0.6, 2)
            weight     = int(size_val * 1.2 + 150) if size_val > 0 else 300

            product = {
                "أسم المنتج":     pname,
                "سعر المنتج":     round(our_price if our_price > 0 else (comp_price - 1), 2),
                "رمز المنتج sku": sku,
                "الوزن":          weight,
                "سعر التكلفة":    cost_price,
                "السعر المخفض":   0,
                "الوصف":          description,
                "image_url":      image_url,
            }
            products.append(product)

    return products


def _extract_product_id_from_row(row):
    """
    استخراج product_id من صف DataFrame — يبحث في كل أسماء الأعمدة الممكنة.
    هذه الدالة تتعامل مع pandas Series (صف من DataFrame).
    """
    # ترتيب الأولوية: الأكثر تحديداً أولاً
    id_columns = [
        # أسماء النتائج من engine.py
        "معرف_المنتج", "معرف المنتج",
        # أسماء سلة الرسمية
        "رقم المنتج", "رقم_المنتج",
        "المعرف", "معرف",
        # أسماء إنجليزية
        "product_id", "Product ID", "Product_ID",
        "ID", "id", "Id",
        "NO", "No", "no", "NUMBER", "number", "Num", "num",
        # SKU
        "SKU", "sku", "Sku",
        "رمز المنتج", "رمز_المنتج", "رمز المنتج sku",
        # أسماء أخرى
        "الكود", "كود", "Code", "code",
        "الرقم", "رقم",
        "Barcode", "barcode", "الباركود",
    ]
    for col in id_columns:
        try:
            val = row.get(col, "")
            if val and str(val) not in ("", "nan", "None", "0", "0.0"):
                return str(val).strip()
        except (ValueError, AttributeError):
            continue
    return ""


def _generate_sku(product_name, brand=""):
    """توليد SKU بسيط من اسم المنتج"""
    import re, hashlib
    brand_code = brand[:3].upper() if brand else "PRF"
    name_hash = hashlib.md5(product_name.encode('utf-8')).hexdigest()[:6].upper()
    return f"{brand_code}-{name_hash}"


def _estimate_weight(size_ml):
    """تقدير الوزن بالجرام من الحجم بالمل"""
    if not size_ml or size_ml <= 0:
        return 300  # وزن افتراضي
    return int(size_ml * 1.2 + 150)
