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
    from engines.engine import extract_brand, extract_size, classify_product
    products = []
    for _, row in df.iterrows():
        if section_type == "update":
            our_price = float(row.get("السعر", 0) or 0)
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            diff = our_price - comp_price
            diff_pct = (diff / comp_price * 100) if comp_price > 0 else 0
            product = {
                "product_name": str(row.get("المنتج", "")),
                "old_price": our_price,
                "new_price": comp_price - 1 if comp_price > 0 else our_price,
                "price_change": round(diff, 2),
                "price_change_pct": round(diff_pct, 2),
                "competitor_name": str(row.get("المنافس", "")).replace('.csv','').replace('.xlsx',''),
                "competitor_price": comp_price,
                "confidence": int(float(row.get("نسبة_التطابق", 0) or 0)),
                "risk_level": str(row.get("الخطورة", "عادي")).replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""),
                "match_stage": str(row.get("مصدر_المطابقة", "fuzzy")),
                "reason": str(row.get("القرار", ""))
            }
        elif section_type == "missing":
            pname = str(row.get("منتج_المنافس", ""))
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            brand = str(row.get("الماركة", "")) or extract_brand(pname)
            size_val = extract_size(pname)
            product = {
                "product_name": pname,
                "competitor_name": str(row.get("المنافس", "")).replace('.csv','').replace('.xlsx',''),
                "competitor_price": comp_price,
                "brand": brand if brand else "Unknown",
                "size": f"{int(size_val)}ml" if size_val else str(row.get("الحجم", "")),
                "type": classify_product(pname),
                "recommendation": "إضافة مقترحة" if comp_price > 0 else "مراجعة",
                "profitability": "عالية" if comp_price > 100 else "متوسطة",
                "suggested_price": comp_price - 1 if comp_price > 0 else 0
            }
        else:  # new
            pname = str(row.get("المنتج", row.get("منتج_المنافس", "")))
            comp_price = float(row.get("سعر_المنافس", 0) or 0)
            brand = str(row.get("الماركة", "")) or extract_brand(pname)
            size_val = extract_size(pname)
            product = {
                "product_name": pname,
                "price": comp_price - 1 if comp_price > 0 else 0,
                "brand": brand if brand else "Unknown",
                "size": f"{int(size_val)}ml" if size_val else str(row.get("الحجم", "")),
                "type": classify_product(pname),
                "competitor_name": str(row.get("المنافس", "")).replace('.csv','').replace('.xlsx',''),
                "competitor_price": comp_price,
                "confidence": int(float(row.get("نسبة_التطابق", 0) or 0)),
                "profitability": "عالية" if comp_price > 100 else "متوسطة",
                "description": f"عطر {brand} {int(size_val)}ml" if brand and size_val else pname,
                "category": "عطور"
            }
        products.append(product)
    return products
