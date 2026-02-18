"""
utils/helpers.py - دوال مساعدة v17.2
الملف الذي كان مفقوداً - يحتوي على جميع الدوال المستوردة في app.py
"""
import pandas as pd
import io
from typing import Optional, Dict, List


# ===== safe_float =====
def safe_float(val, default=0.0) -> float:
    """تحويل قيمة إلى float بأمان"""
    try:
        if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ===== format_price =====
def format_price(price, currency="ر.س") -> str:
    """تنسيق عرض السعر"""
    try:
        return f"{float(price):,.0f} {currency}"
    except:
        return f"0 {currency}"


# ===== format_diff =====
def format_diff(diff) -> str:
    """تنسيق عرض فرق السعر"""
    try:
        d = float(diff)
        sign = "+" if d > 0 else ""
        return f"{sign}{d:,.0f} ر.س"
    except:
        return "0 ر.س"


# ===== get_filter_options =====
def get_filter_options(df: pd.DataFrame) -> dict:
    """استخراج خيارات الفلاتر من DataFrame"""
    opts = {
        "brands": ["الكل"],
        "competitors": ["الكل"],
        "types": ["الكل"],
    }
    if df is None or df.empty:
        return opts

    # الماركات
    if "الماركة" in df.columns:
        brands = df["الماركة"].dropna().unique().tolist()
        brands = sorted([str(b) for b in brands if str(b).strip() and str(b) != "nan"])
        opts["brands"] = ["الكل"] + brands

    # المنافسون
    if "المنافس" in df.columns:
        comps = df["المنافس"].dropna().unique().tolist()
        comps = sorted([str(c) for c in comps if str(c).strip() and str(c) != "nan"])
        opts["competitors"] = ["الكل"] + comps

    # الأنواع
    if "النوع" in df.columns:
        types = df["النوع"].dropna().unique().tolist()
        types = sorted([str(t) for t in types if str(t).strip() and str(t) != "nan"])
        opts["types"] = ["الكل"] + types

    return opts


# ===== apply_filters =====
def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """تطبيق الفلاتر على DataFrame"""
    if df is None or df.empty:
        return df

    result = df.copy()

    # بحث نصي
    search = filters.get("search", "").strip()
    if search:
        mask = pd.Series([False] * len(result))
        for col in ["المنتج", "منتج المنافس", "الماركة"]:
            if col in result.columns:
                mask = mask | result[col].astype(str).str.contains(search, case=False, na=False)
        result = result[mask]

    # فلتر الماركة
    brand = filters.get("brand", "الكل")
    if brand and brand != "الكل" and "الماركة" in result.columns:
        result = result[result["الماركة"].astype(str) == brand]

    # فلتر المنافس
    competitor = filters.get("competitor", "الكل")
    if competitor and competitor != "الكل" and "المنافس" in result.columns:
        result = result[result["المنافس"].astype(str) == competitor]

    # فلتر النوع
    ptype = filters.get("type", "الكل")
    if ptype and ptype != "الكل" and "النوع" in result.columns:
        result = result[result["النوع"].astype(str) == ptype]

    # فلتر نسبة التطابق
    match_min = filters.get("match_min")
    if match_min and "نسبة التطابق" in result.columns:
        result = result[result["نسبة التطابق"] >= float(match_min)]

    # فلتر أقل سعر
    price_min = filters.get("price_min", 0.0)
    if price_min and price_min > 0 and "السعر" in result.columns:
        result = result[result["السعر"] >= float(price_min)]

    # فلتر أعلى سعر
    price_max = filters.get("price_max")
    if price_max and price_max > 0 and "السعر" in result.columns:
        result = result[result["السعر"] <= float(price_max)]

    return result.reset_index(drop=True)


# ===== export_to_excel =====
def export_to_excel(df: pd.DataFrame, sheet_name: str = "النتائج") -> bytes:
    """تصدير DataFrame إلى Excel"""
    output = io.BytesIO()
    export_df = df.copy()

    # إزالة الأعمدة غير القابلة للتسلسل
    for col in ["جميع المنافسين"]:
        if col in export_df.columns:
            export_df = export_df.drop(columns=[col])

    safe_name = sheet_name[:31]
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, sheet_name=safe_name, index=False)

        # تنسيق العمود
        ws = writer.sheets[safe_name]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    return output.getvalue()


# ===== export_multiple_sheets =====
def export_multiple_sheets(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """تصدير عدة DataFrames في ملف Excel متعدد الأوراق"""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            export_df = df.copy()
            for col in ["جميع المنافسين"]:
                if col in export_df.columns:
                    export_df = export_df.drop(columns=[col])

            safe_name = str(sheet_name)[:31]
            export_df.to_excel(writer, sheet_name=safe_name, index=False)

            # تنسيق تلقائي
            ws = writer.sheets[safe_name]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    return output.getvalue()


# ===== parse_pasted_text =====
def parse_pasted_text(text: str):
    """
    تحليل نص ملصوق وتحويله إلى DataFrame
    يدعم: CSV، TSV، جداول مفصولة بـ |
    """
    if not text or not text.strip():
        return None, "النص فارغ"

    lines = [l.strip() for l in text.strip().split('\n') if l.strip()]

    if not lines:
        return None, "لا توجد بيانات"

    # محاولة 1: مفصول بـ |
    if '|' in lines[0]:
        rows = []
        for line in lines:
            if set(line.replace(' ', '').replace('-', '')) == {'|'}:
                continue  # تخطي خطوط الفاصل
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if cells:
                rows.append(cells)

        if len(rows) >= 2:
            try:
                df = pd.DataFrame(rows[1:], columns=rows[0])
                return df, f"✅ تم تحليل {len(df)} صف"
            except:
                pass

    # محاولة 2: TSV (tabs)
    if '\t' in lines[0]:
        try:
            df = pd.read_csv(io.StringIO(text), sep='\t')
            return df, f"✅ تم تحليل {len(df)} صف (TSV)"
        except:
            pass

    # محاولة 3: CSV
    try:
        df = pd.read_csv(io.StringIO(text))
        return df, f"✅ تم تحليل {len(df)} صف (CSV)"
    except:
        pass

    # محاولة 4: كل سطر منتج
    if len(lines) >= 2:
        df = pd.DataFrame({"البيانات": lines})
        return df, f"✅ تم تحليل {len(df)} سطر"

    return None, "❌ لا يمكن تحليل الصيغة. جرب CSV أو جدول مفصول بـ |"


# ===== BackgroundTask (stub) =====
class BackgroundTask:
    """
    محاكاة معالجة في الخلفية
    ملاحظة: Streamlit لا يدعم true background threads بشكل كامل
    هذا placeholder وظيفي
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.done = False
        self.error = None

    def run(self):
        """تشغيل المهمة مباشرة (synchronous)"""
        try:
            self.result = self.func(*self.args, **self.kwargs)
            self.done = True
        except Exception as e:
            self.error = str(e)
            self.done = True
        return self.result

    def is_done(self):
        return self.done
