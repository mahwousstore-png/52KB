"""
utils/make_helper.py - دوال إرسال البيانات إلى Make.com v2.0
✅ إرسال السعر + اسم المنتج + رقم المنتج إلى سلة عبر Make Webhooks
✅ دعم إرسال منتج واحد أو مجموعة منتجات
✅ معالجة أخطاء شاملة
"""
import requests
import json
import os
from typing import List, Dict, Any, Optional


# ── قراءة Webhook URLs من البيئة أو القيم الافتراضية ──────────────────────
def _get_webhook_url(key: str, default: str) -> str:
    return os.environ.get(key, "") or default


WEBHOOK_UPDATE_PRICES = _get_webhook_url(
    "WEBHOOK_UPDATE_PRICES",
    "https://hook.eu2.make.com/8jia6gc7s1cpkeg6catlrvwck768sbfk"
)
WEBHOOK_NEW_PRODUCTS = _get_webhook_url(
    "WEBHOOK_NEW_PRODUCTS",
    "https://hook.eu2.make.com/xvubj23dmpxu8qzilstd25cnumrwtdxm"
)

TIMEOUT = 15  # ثانية


# ── دالة الإرسال الأساسية ─────────────────────────────────────────────────
def _post_to_webhook(url: str, payload: Any) -> Dict:
    """
    إرسال بيانات JSON إلى Webhook URL.
    يُعيد dict: {"success": bool, "message": str, "status_code": int}
    """
    if not url:
        return {"success": False, "message": "❌ لم يتم تحديد Webhook URL", "status_code": 0}

    try:
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)

        if resp.status_code in (200, 201, 202, 204):
            return {
                "success": True,
                "message": f"✅ تم الإرسال بنجاح ({resp.status_code})",
                "status_code": resp.status_code,
            }
        else:
            return {
                "success": False,
                "message": f"❌ خطأ HTTP {resp.status_code}: {resp.text[:200]}",
                "status_code": resp.status_code,
            }
    except requests.exceptions.Timeout:
        return {"success": False, "message": "❌ انتهت مهلة الاتصال (Timeout)", "status_code": 0}
    except requests.exceptions.ConnectionError:
        return {"success": False, "message": "❌ فشل الاتصال بـ Make — تحقق من الإنترنت", "status_code": 0}
    except Exception as e:
        return {"success": False, "message": f"❌ خطأ غير متوقع: {str(e)}", "status_code": 0}


# ── تحويل DataFrame إلى قائمة منتجات لـ Make ─────────────────────────────
def export_to_make_format(df, section_type: str = "update") -> List[Dict]:
    """
    تحويل DataFrame إلى قائمة منتجات جاهزة للإرسال إلى Make.
    كل منتج يحتوي على:
      - product_id : رقم المنتج في سلة
      - name       : اسم المنتج
      - price      : السعر الجديد
      - section    : نوع القسم (update / new / missing)
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return []

    products = []

    for _, row in df.iterrows():
        # ── رقم المنتج ──────────────────────────────────────────────────
        # نبحث في جميع الأعمدة المحتملة لرقم المنتج
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or
            row.get("sku", "") or
            row.get("SKU", "") or ""
        )
        # تحويل float إلى int لإزالة .0 (مثل 1081786650.0 → 1081786650)
        try:
            _fv = float(_pid_raw)
            product_id = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw).strip()
        except (ValueError, TypeError):
            product_id = str(_pid_raw).strip()
        if product_id in ("", "nan", "None", "NaN"): product_id = ""

        # ــ اسم المنتج ـــــــــــــــــــــــــــــــــــــــــــــــ
        name = (
            str(row.get("المنتج", ""))
            or str(row.get("منتج_المنافس", ""))  # للمنتجات المفقودة
            or str(row.get("أسم المنتج", ""))
            or str(row.get("اسم المنتج", ""))
            or str(row.get("name", ""))
            or ""
        )
        name = name.strip() if name not in ("", "nan", "None") else ""

                # ــ السعر ـــــــــــــــــــــــــــــــــــــــــــــــ
        comp_price = _safe_float(row.get("سعر_المنافس", 0))
        our_price  = _safe_float(row.get("السعر", 0) or row.get("سعر المنتج", 0) or row.get("price", 0) or 0)

        if section_type == "raise":
            # سعرنا أعلى من المنافس → نخفض سعرنا إلى سعر المنافس - 1 ريال
            if comp_price > 0:
                price = round(comp_price - 1, 2)
            else:
                price = our_price
        elif section_type == "lower":
            # سعرنا أقل من المنافس → نرفع سعرنا إلى سعر المنافس - 1 ريال (أقل منه بريال)
            if comp_price > 0:
                price = round(comp_price - 1, 2)
            else:
                price = our_price
        elif section_type in ("approved", "update"):
            # سعر موافق عليه → نرسل سعرنا الحالي كما هو
            price = our_price
        else:
            # منتجات جديدة أو مفقودة: نرسل سعر المنافس
            price = comp_price if comp_price > 0 else our_price

        # ── تجميع بيانات المنتج ─────────────────────────────────────────
        product = {
            "product_id": product_id,
            "name": name,
            "price": price,
            "section": section_type,
        }

        # حقول إضافية اختيارية
        comp_name  = str(row.get("منتج_المنافس", ""))
        comp_src   = str(row.get("المنافس", ""))
        diff       = _safe_float(row.get("الفرق", 0))
        match_pct  = _safe_float(row.get("نسبة_التطابق", 0))
        decision   = str(row.get("القرار", ""))
        brand      = str(row.get("الماركة", ""))

        if comp_name and comp_name not in ("nan", "None", "—"):
            product["comp_name"] = comp_name
        if comp_src and comp_src not in ("nan", "None"):
            product["competitor"] = comp_src
        if diff:
            product["price_diff"] = diff
        if match_pct:
            product["match_score"] = match_pct
        if decision and decision not in ("nan", "None"):
            product["decision"] = decision
        if brand and brand not in ("nan", "None"):
            product["brand"] = brand

        products.append(product)

    return products


# ── إرسال منتج واحد ──────────────────────────────────────────────────────
def send_single_product(product: Dict) -> Dict:
    """
    إرسال منتج واحد إلى Make webhook.
    product يجب أن يحتوي على:
      - product_id : رقم المنتج
      - name       : اسم المنتج
      - price      : السعر
    """
    if not product:
        return {"success": False, "message": "❌ لا توجد بيانات للإرسال"}

    # التحقق من الحقول المطلوبة
    name = str(product.get("name", "")).strip()
    price = _safe_float(product.get("price", 0))
    product_id = str(product.get("product_id", "")).strip()

    if not name:
        return {"success": False, "message": "❌ اسم المنتج مطلوب"}
    if price <= 0:
        return {"success": False, "message": f"❌ السعر غير صحيح: {price}"}

    payload = {
        "products": [{
            "product_id": product_id,
            "name": name,
            "price": price,
            "section": product.get("section", "update"),
            "comp_name": product.get("comp_name", ""),
            "competitor": product.get("competitor", ""),
            "price_diff": product.get("diff", 0),
            "match_score": product.get("match_score", 0),
            "decision": product.get("decision", ""),
            "brand": product.get("brand", ""),
        }]
    }

    result = _post_to_webhook(WEBHOOK_UPDATE_PRICES, payload)
    if result["success"]:
        result["message"] = f"✅ تم إرسال «{name}» (السعر: {price:,.0f} ر.س) إلى Make"
    return result


# ── إرسال تحديثات الأسعار ────────────────────────────────────────────────
def send_price_updates(products: List[Dict]) -> Dict:
    """
    إرسال قائمة منتجات لتحديث أسعارها في سلة عبر Make.
    كل منتج: product_id + name + price
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}

    # تنظيف وتحقق من البيانات
    valid_products = []
    skipped = 0
    for p in products:
        name  = str(p.get("name", "")).strip()
        price = _safe_float(p.get("price", 0))
        pid   = str(p.get("product_id", "")).strip()

        if not name or price <= 0:
            skipped += 1
            continue

        valid_products.append({
            "product_id": pid,
            "name": name,
            "price": price,
            "section": p.get("section", "update"),
            "comp_name": p.get("comp_name", ""),
            "competitor": p.get("competitor", ""),
            "price_diff": p.get("price_diff", p.get("diff", 0)),
            "match_score": p.get("match_score", 0),
            "decision": p.get("decision", ""),
            "brand": p.get("brand", ""),
        })

    if not valid_products:
        return {
            "success": False,
            "message": f"❌ لا توجد منتجات صالحة (تم تخطي {skipped} منتج)"
        }

    payload = {"products": valid_products}
    result = _post_to_webhook(WEBHOOK_UPDATE_PRICES, payload)

    if result["success"]:
        skip_msg = f" (تم تخطي {skipped})" if skipped else ""
        result["message"] = f"✅ تم إرسال {len(valid_products)} منتج لتحديث الأسعار{skip_msg}"
    return result


# ── إرسال منتجات جديدة ───────────────────────────────────────────────────
def send_new_products(products: List[Dict]) -> Dict:
    """
    إرسال قائمة منتجات جديدة إلى Make لإضافتها في سلة.
    يُرسل كل منتج في طلب منفصل كـ {"data": [{...}]}
    Iterator في Make يقرأ data[] ويُخرج كل عنصر لـ Salla.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}
    sent = 0
    skipped = 0
    errors = []
    for p in products:
        name  = str(p.get("name", p.get("أسم المنتج", ""))).strip()
        price = _safe_float(
            p.get("price", 0)
            or p.get("سعر المنتج", 0)
            or p.get("السعر", 0)
        )
        pid   = str(p.get("product_id", p.get("معرف_المنتج", ""))).strip()
        if not name:
            skipped += 1
            continue
        payload = {
            "name": name,
            "price": price,
            "product_type": "product",
            "sku": "",
            "weight": 0.1,
            "weight_type": "kg",
            "quantity": 100,
            "require_shipping": True,
            "maximum_quantity_per_order": 1,
            "categories": [],
            "description": str(p.get("الوصف", p.get("description", ""))).strip(),
            "cost_price": _safe_float(p.get("cost_price", 0)),
            "sale_price": _safe_float(p.get("sale_price", 0)),
        }
        result = _post_to_webhook(WEBHOOK_NEW_PRODUCTS, payload)
        if result["success"]:
            sent += 1
        else:
            errors.append(name)
    if sent == 0:
        return {"success": False, "message": f"❌ فشل إرسال جميع المنتجات. تم تخطي {skipped}"}
    skip_msg = f" (تم تخطي {skipped})" if skipped else ""
    err_msg  = f" (فشل {len(errors)})" if errors else ""
    return {"success": True, "message": f"✅ تم إرسال {sent} منتج جديد إلى Make{skip_msg}{err_msg}"}


# ── إرسال المنتجات المفقودة ─────────────────────────────────────────────
def send_missing_products(products: List[Dict]) -> Dict:
    """
    إرسال قائمة المنتجات المفقودة إلى Make.
    يُرسل كل منتج في طلب منفصل كـ {"data": [{...}]}
    Iterator في Make يقرأ data[] ويُخرج كل عنصر لـ Salla.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات مفقودة للإرسال"}
    sent = 0
    skipped = 0
    errors = []
    for p in products:
        name  = str(p.get("name", p.get("المنتج", ""))).strip()
        price = _safe_float(p.get("price", p.get("السعر", 0)))
        pid   = str(p.get("product_id", p.get("معرف_المنتج", ""))).strip()
        if not name:
            skipped += 1
            continue
        payload = {
            "name": name,
            "price": price,
            "product_type": "product",
            "sku": "",
            "weight": 0.1,
            "weight_type": "kg",
            "quantity": 100,
            "require_shipping": True,
            "maximum_quantity_per_order": 1,
            "categories": [],
            "description": str(p.get("الوصف", p.get("description", ""))).strip(),
            "cost_price": _safe_float(p.get("cost_price", 0)),
            "sale_price": _safe_float(p.get("sale_price", 0)),
        }
        result = _post_to_webhook(WEBHOOK_NEW_PRODUCTS, payload)
        if result["success"]:
            sent += 1
        else:
            errors.append(name)
    if sent == 0:
        return {"success": False, "message": f"❌ فشل إرسال جميع المنتجات. تم تخطي {skipped}"}
    skip_msg = f" (تم تخطي {skipped})" if skipped else ""
    err_msg  = f" (فشل {len(errors)})" if errors else ""
    return {"success": True, "message": f"✅ تم إرسال {sent} منتج مفقود إلى Make{skip_msg}{err_msg}"}


# ── فحص حالة الاتصال بـ Webhooks ─────────────────────────────────────────
def verify_webhook_connection() -> Dict:
    """
    فحص حالة الاتصال بجميع Webhooks.
    يُعيد dict: {"update_prices": {...}, "new_products": {...}, "all_connected": bool}
    """
    test_payload = {
        "products": [{
            "product_id": "test-001",
            "name": "اختبار الاتصال",
            "price": 1.0,
            "section": "test",
        }],
        "test": True
    }

    results = {}

    # فحص Webhook تحديث الأسعار
    r1 = _post_to_webhook(WEBHOOK_UPDATE_PRICES, test_payload)
    results["update_prices"] = {
        "success": r1["success"],
        "message": r1["message"],
        "url": WEBHOOK_UPDATE_PRICES[:50] + "..." if len(WEBHOOK_UPDATE_PRICES) > 50 else WEBHOOK_UPDATE_PRICES,
    }

    # فحص Webhook المنتجات الجديدة
    r2 = _post_to_webhook(WEBHOOK_NEW_PRODUCTS, test_payload)
    results["new_products"] = {
        "success": r2["success"],
        "message": r2["message"],
        "url": WEBHOOK_NEW_PRODUCTS[:50] + "..." if len(WEBHOOK_NEW_PRODUCTS) > 50 else WEBHOOK_NEW_PRODUCTS,
    }

    results["all_connected"] = r1["success"] and r2["success"]
    return results


# ── دالة مساعدة داخلية ───────────────────────────────────────────────────
def _safe_float(val, default: float = 0.0) -> float:
    """تحويل آمن إلى float"""
    try:
        if val is None or val == "" or str(val) in ("nan", "None", "NaN"):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default
