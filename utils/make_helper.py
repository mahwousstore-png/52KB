"""
make_helper.py - أتمتة Make.com v17.0
- Webhooks أصلية مربوطة مباشرة
- دوال تصدير لكل قسم
"""
import requests, json, time
from datetime import datetime
from config import WEBHOOK_UPDATE_PRICES, WEBHOOK_NEW_PRODUCTS


def send_price_updates(products, webhook_url=None):
    """إرسال تحديثات الأسعار إلى Make.com"""
    url = webhook_url or WEBHOOK_UPDATE_PRICES
    try:
        payload = {
            "type": "price_updates",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(products),
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
    """إرسال منتجات جديدة/مفقودة إلى Make.com"""
    url = webhook_url or WEBHOOK_NEW_PRODUCTS
    try:
        payload = {
            "type": "new_products",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(products),
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
    """إرسال المنتجات المفقودة إلى Make.com"""
    url = webhook_url or WEBHOOK_NEW_PRODUCTS
    try:
        payload = {
            "type": "missing_products",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(products),
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
    """تحويل DataFrame إلى صيغة Make"""
    products = []
    for _, row in df.iterrows():
        product = {
            "name": str(row.get("المنتج", "")),
            "our_price": float(row.get("السعر", 0)),
            "comp_name": str(row.get("اسم المنافس", "")),
            "comp_price": float(row.get("أقل سعر منافس", 0)),
            "diff": float(row.get("الفرق", 0)),
            "match_score": float(row.get("نسبة التطابق", 0)),
            "decision": str(row.get("القرار", "")),
            "brand": str(row.get("الماركة", "")),
            "competitor": str(row.get("المنافس", ""))
        }
        products.append(product)
    return products
