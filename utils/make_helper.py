"""
make_helper.py - أتمتة Make.com v17.0
- Webhooks أصلية مربوطة مباشرة
- دوال تصدير لكل قسم
"""
import requests, json, time
from datetime import datetime
from config import WEBHOOK_UPDATE_PRICES, WEBHOOK_NEW_PRODUCTS


def send_price_updates(products, webhook_url=None):
    """إرسال تحديثات الأسعار إلى Make.com بتنسيق MAKE_INTEGRATION_GUIDE"""
    url = webhook_url or WEBHOOK_UPDATE_PRICES
    try:
        payload = {
            "event_type": "price_update",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "perfume_pricing_system_v16",
            "total_products": len(products),
            "products": products
        }
        resp = requests.post(url, json=payload, timeout=15)
        return {
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "message": f"تم إرسال {len(products)} منتج بنجاح" if resp.status_code == 200 else f"خطأ: {resp.status_code}"
        }
    except Exception as e:
        return {"success": False, "status_code": 0, "message": f"خطأ: {str(e)}"}


def send_new_products(products, webhook_url=None):
    """إرسال منتجات جديدة إلى Make.com بتنسيق MAKE_INTEGRATION_GUIDE"""
    url = webhook_url or WEBHOOK_NEW_PRODUCTS
    try:
        payload = {
            "event_type": "new_products",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "perfume_pricing_system_v16",
            "total_products": len(products),
            "products": products
        }
        resp = requests.post(url, json=payload, timeout=15)
        return {
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "message": f"تم إرسال {len(products)} منتج جديد بنجاح" if resp.status_code == 200 else f"خطأ: {resp.status_code}"
        }
    except Exception as e:
        return {"success": False, "status_code": 0, "message": f"خطأ: {str(e)}"}


def send_missing_products(products, webhook_url=None):
    """إرسال المنتجات المفقودة إلى Make.com بتنسيق MAKE_INTEGRATION_GUIDE"""
    url = webhook_url or WEBHOOK_NEW_PRODUCTS
    try:
        payload = {
            "event_type": "missing_products",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "perfume_pricing_system_v16",
            "total_products": len(products),
            "products": products
        }
        resp = requests.post(url, json=payload, timeout=15)
        return {
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "message": f"تم إرسال {len(products)} منتج مفقود بنجاح" if resp.status_code == 200 else f"خطأ: {resp.status_code}"
        }
    except Exception as e:
        return {"success": False, "status_code": 0, "message": f"خطأ: {str(e)}"}


def send_to_make(data, webhook_type="update"):
    """دالة عامة للإرسال إلى Make"""
    if webhook_type == "update":
        return send_price_updates(data)
    elif webhook_type in ["new", "missing"]:
        return send_new_products(data)
    return {"success": False, "message": "نوع غير معروف"}


def send_single_product(product, action="update"):
    """إرسال منتج واحد إلى Make"""
    return send_to_make([product], action)


def test_webhook(webhook_type="update"):
    """اختبار اتصال Webhook"""
    url = WEBHOOK_UPDATE_PRICES if webhook_type == "update" else WEBHOOK_NEW_PRODUCTS
    try:
        payload = {"type": "test", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        resp = requests.post(url, json=payload, timeout=10)
        return {
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "url": url,
            "message": "الاتصال ناجح ✅" if resp.status_code == 200 else f"فشل الاتصال: {resp.status_code}"
        }
    except Exception as e:
        return {"success": False, "url": url, "message": f"خطأ: {str(e)}"}


def verify_webhook_connection():
    """التحقق من جميع الاتصالات"""
    results = {
        "update_prices": test_webhook("update"),
        "new_products": test_webhook("new")
    }
    results["all_connected"] = all(r["success"] for r in results.values())
    return results


def export_to_make_format(df, section_type="update"):
    """تحويل DataFrame إلى صيغة Make حسب MAKE_INTEGRATION_GUIDE"""
    products = []
    for _, row in df.iterrows():
        our_price = float(row.get("السعر", 0))
        comp_price = float(row.get("سعر_المنافس", 0))
        diff = our_price - comp_price
        diff_pct = (diff / comp_price * 100) if comp_price > 0 else 0
        
        product = {
            "product_name": str(row.get("المنتج", "")),
            "old_price": our_price,
            "new_price": comp_price - 1 if comp_price > 0 else our_price,  # سعر مقترح
            "price_change": diff,
            "price_change_pct": round(diff_pct, 2),
            "competitor_name": str(row.get("المنافس", "")),
            "competitor_price": comp_price,
            "confidence": int(row.get("نسبة_التطابق", 0)),
            "risk_level": str(row.get("الخطورة", "عادي")),
            "match_stage": str(row.get("مصدر_المطابقة", "fuzzy")),
            "reason": str(row.get("القرار", ""))
        }
        products.append(product)
    return products
