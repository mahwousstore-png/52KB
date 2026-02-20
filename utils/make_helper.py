"""
utils/make_helper.py - دوال تكامل Make (Integromat)
إرسال البيانات عبر Webhooks إلى سيناريوهات Make
"""
import requests
from typing import List, Dict, Any

try:
    from config import WEBHOOK_UPDATE_PRICES, WEBHOOK_NEW_PRODUCTS
except ImportError:
    WEBHOOK_UPDATE_PRICES = ""
    WEBHOOK_NEW_PRODUCTS = ""

_TIMEOUT = 15
_SKIP_COLUMNS = ["جميع المنافسين", "جميع_المنافسين"]


def export_to_make_format(df, section_type: str = "update") -> List[Dict[str, Any]]:
    """تحويل DataFrame إلى قائمة قواميس لإرسالها لـ Make"""
    if df is None or df.empty:
        return []
    records = df.copy()
    for col in _SKIP_COLUMNS:
        if col in records.columns:
            records = records.drop(columns=[col])
    products = records.to_dict(orient="records")
    for p in products:
        p["section_type"] = section_type
        for k, v in p.items():
            if hasattr(v, "item"):
                p[k] = v.item()
    return products


def _send_to_webhook(webhook_url: str, products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """إرسال قائمة منتجات إلى Webhook محدد"""
    if not webhook_url:
        return {"success": False, "message": "❌ لم يتم تحديد Webhook URL"}
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}
    try:
        response = requests.post(
            webhook_url,
            json={"products": products, "count": len(products)},
            timeout=_TIMEOUT,
        )
        if response.status_code in (200, 201, 202, 204):
            return {"success": True, "message": f"✅ تم إرسال {len(products)} منتج بنجاح"}
        return {
            "success": False,
            "message": f"❌ فشل الإرسال — كود الاستجابة: {response.status_code}",
        }
    except requests.exceptions.Timeout:
        return {"success": False, "message": "❌ انتهت مهلة الاتصال بـ Make"}
    except requests.exceptions.ConnectionError:
        return {"success": False, "message": "❌ تعذّر الاتصال بـ Make"}
    except Exception as e:
        return {"success": False, "message": f"❌ خطأ: {e}"}


def send_price_updates(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """إرسال تحديثات الأسعار إلى Make"""
    return _send_to_webhook(WEBHOOK_UPDATE_PRICES, products)


def send_new_products(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """إرسال المنتجات الجديدة إلى Make"""
    return _send_to_webhook(WEBHOOK_NEW_PRODUCTS, products)


def send_missing_products(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """إرسال المنتجات المفقودة إلى Make"""
    return _send_to_webhook(WEBHOOK_NEW_PRODUCTS, products)


def send_single_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """إرسال منتج واحد إلى Make"""
    return _send_to_webhook(WEBHOOK_UPDATE_PRICES, [product])


def verify_webhook_connection() -> Dict[str, Any]:
    """التحقق من اتصال Webhooks"""
    results: Dict[str, Any] = {}
    webhooks = {
        "تحديث الأسعار": WEBHOOK_UPDATE_PRICES,
        "منتجات جديدة": WEBHOOK_NEW_PRODUCTS,
    }
    for name, url in webhooks.items():
        if not url:
            results[name] = {"success": False, "message": "❌ لم يتم تحديد Webhook URL"}
            continue
        try:
            response = requests.post(
                url,
                json={"ping": True},
                timeout=_TIMEOUT,
            )
            if response.status_code in (200, 201, 202, 204):
                results[name] = {"success": True, "message": "🟢 متصل"}
            else:
                results[name] = {
                    "success": False,
                    "message": f"🔴 كود الاستجابة: {response.status_code}",
                }
        except requests.exceptions.Timeout:
            results[name] = {"success": False, "message": "🔴 انتهت مهلة الاتصال"}
        except requests.exceptions.ConnectionError:
            results[name] = {"success": False, "message": "🔴 تعذّر الاتصال"}
        except Exception as e:
            results[name] = {"success": False, "message": f"🔴 خطأ: {e}"}
    results["all_connected"] = bool(webhooks) and all(
        r["success"] for r in results.values() if isinstance(r, dict)
    )
    return results
