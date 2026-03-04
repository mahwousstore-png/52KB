"""
app.py - نظام التسعير الذكي مهووس v19.0
✅ معالجة خلفية مع حفظ تلقائي
✅ جداول مقارنة بصرية في كل الأقسام
✅ أزرار AI + قرارات لكل منتج
✅ بحث أسعار السوق والمنافسين
✅ بحث mahwous.com للمنتجات المفقودة
✅ تحديث تلقائي للأسعار عند إعادة رفع المنافس
✅ تصدير Make لكل منتج وللمجموعات
✅ Gemini Chat مباشر
✅ فلاتر ذكية في كل قسم
✅ تاريخ جميل لكل العمليات
"""
import streamlit as st
import pandas as pd
import threading
import time
import uuid
from datetime import datetime

from config import *
from styles import get_styles, stat_card, vs_card
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             extract_brand, extract_size, extract_type, is_sample)
from engines.ai_engine import (call_ai, gemini_chat, chat_with_ai,
                                verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate, process_paste,
                                fetch_fragrantica_info, generate_mahwous_description,
                                analyze_paste)
from utils.helpers import (apply_filters, get_filter_options, export_to_excel,
                            export_multiple_sheets, parse_pasted_text,
                            safe_float, format_price, format_diff)
from utils.make_helper import (send_price_updates, send_new_products,
                                send_missing_products, send_single_product,
                                verify_webhook_connection, export_to_make_format)
from utils.db_manager import (init_db, log_event, log_decision,
                               log_analysis, get_events, get_decisions,
                               get_analysis_history, upsert_price_history,
                               get_price_history, get_price_changes,
                               save_job_progress, get_job_progress, get_last_job,
                               save_hidden_product, get_hidden_product_keys)

# ── إعداد الصفحة ──────────────────────────
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON,
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(get_styles(), unsafe_allow_html=True)
init_db()

# ── Session State ─────────────────────────
_defaults = {
    "results": None, "missing_df": None, "analysis_df": None,
    "chat_history": [], "job_id": None, "job_running": False,
    "decisions_pending": {},   # {product_name: action}
    "our_df": None, "comp_dfs": None,  # حفظ الملفات للمنتجات المفقودة
    "hidden_products": set(),  # منتجات أُرسلت لـ Make أو أُزيلت
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ── دوال مساعدة ───────────────────────────
def db_log(page, action, details=""):
    try: log_event(page, action, details)
    except: pass

def ts_badge(ts_str=""):
    """شارة تاريخ مصغرة جميلة"""
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f'<span style="font-size:.65rem;color:#555;background:#1a1a2e;padding:1px 6px;border-radius:8px;margin-right:4px">🕐 {ts_str}</span>'

def decision_badge(action):
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed":  ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


# ════════════════════════════════════════════════
#  المعالجة الخلفية
# ════════════════════════════════════════════════
def _run_analysis_background(job_id, our_df, comp_dfs, our_file_name, comp_names):
    """تعمل في thread منفصل — تحفظ التقدم كل 10 منتجات"""
    total = len(our_df)
    processed = 0
    partial_results = []

    def progress_cb(pct):
        nonlocal processed
        processed = int(pct * total)
        if processed % 10 == 0 or processed >= total:
            save_job_progress(job_id, total, processed,
                              partial_results, "running",
                              our_file_name, comp_names)

    try:
        analysis_df = run_full_analysis(our_df, comp_dfs,
                                        progress_callback=progress_cb)
        # حفظ تاريخ الأسعار
        for _, row in analysis_df.iterrows():
            if row.get("نسبة_التطابق", 0) > 0:
                upsert_price_history(
                    str(row.get("المنتج", "")),
                    str(row.get("المنافس", "")),
                    safe_float(row.get("سعر_المنافس", 0)),
                    safe_float(row.get("السعر", 0)),
                    safe_float(row.get("الفرق", 0)),
                    safe_float(row.get("نسبة_التطابق", 0)),
                    str(row.get("القرار", ""))
                )

        missing_df = find_missing_products(our_df, comp_dfs)
        results = {
            "price_raise": analysis_df[analysis_df["القرار"].str.contains("أعلى", na=False)].reset_index(drop=True),
            "price_lower": analysis_df[analysis_df["القرار"].str.contains("أقل",  na=False)].reset_index(drop=True),
            "approved":    analysis_df[analysis_df["القرار"].str.contains("موافق",na=False)].reset_index(drop=True),
            "review":      analysis_df[analysis_df["القرار"].str.contains("مراجعة",na=False)].reset_index(drop=True),
            "missing": missing_df,
            "all":     analysis_df,
        }
        save_job_progress(job_id, total, total,
                          analysis_df.to_dict("records"),
                          "done", our_file_name, comp_names,
                          missing=missing_df.to_dict("records") if not missing_df.empty else [])
        log_analysis(our_file_name, comp_names, total,
                     len(analysis_df[analysis_df["نسبة_التطابق"] > 0]),
                     len(missing_df))

    except Exception as e:
        save_job_progress(job_id, total, processed,
                          [], f"error: {str(e)}", our_file_name, comp_names)


# ════════════════════════════════════════════════
#  مكوّن جدول المقارنة البصري (مشترك)
# ════════════════════════════════════════════════
def render_pro_table(df, prefix, section_type="update", show_search=True):
    """
    جدول احترافي بصري مع:
    - فلاتر ذكية
    - أزرار AI + قرار لكل منتج
    - تصدير Make
    - Pagination
    """
    if df is None or df.empty:
        st.info("لا توجد منتجات")
        return

    # ── فلاتر ─────────────────────────────────
    opts = get_filter_options(df)
    with st.expander("🔍 فلاتر متقدمة", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        search   = c1.text_input("🔎 بحث",    key=f"{prefix}_s")
        brand_f  = c2.selectbox("🏷️ الماركة", opts["brands"],      key=f"{prefix}_b")
        comp_f   = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
        type_f   = c4.selectbox("🧴 النوع",   opts["types"],       key=f"{prefix}_t")
        c5, c6, c7 = st.columns(3)
        match_min  = c5.slider("أقل تطابق%", 0, 100, 0, key=f"{prefix}_m")
        price_min  = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
        price_max  = c7.number_input("سعر لـ", 0.0, key=f"{prefix}_p2")

    filters = {
        "search": search, "brand": brand_f, "competitor": comp_f,
        "type": type_f,
        "match_min": match_min if match_min > 0 else None,
        "price_min": price_min if price_min > 0 else 0.0,
        "price_max": price_max if price_max > 0 else None,
    }
    filtered = apply_filters(df, filters)

    # ── شريط الأدوات ───────────────────────────
    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
    with ac1:
        _exdf = filtered.copy()
        if "جميع المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع_المنافسين"])
        excel_data = export_to_excel(_exdf, prefix)
        st.download_button("📥 Excel", data=excel_data,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{prefix}_xl")
    with ac2:
        _csdf = filtered.copy()
        if "جميع المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع_المنافسين"])
        _csv_bytes = _csdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📄 CSV", data=_csv_bytes,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"{prefix}_csv")
    with ac3:
        _bulk_labels = {"raise": "🤖 تحليل ذكي — خفض (أول 20)",
                        "lower": "🤖 تحليل ذكي — رفع (أول 20)",
                        "review": "🤖 تحقق جماعي (أول 20)",
                        "approved": "🤖 مراجعة (أول 20)"}
        if st.button(_bulk_labels.get(prefix, "🤖 AI جماعي (أول 20)"), key=f"{prefix}_bulk"):
            with st.spinner("🤖 AI يحلل البيانات..."):
                _section_map = {"raise": "price_raise", "lower": "price_lower",
                                "review": "review", "approved": "approved"}
                items = [{
                    "our": str(r.get("المنتج", "")),
                    "comp": str(r.get("منتج_المنافس", "")),
                    "our_price": safe_float(r.get("السعر", 0)),
                    "comp_price": safe_float(r.get("سعر_المنافس", 0))
                } for _, r in filtered.head(20).iterrows()]
                res = bulk_verify(items, _section_map.get(prefix, "general"))
                st.markdown(f'<div class="ai-box">{res["response"]}</div>',
                            unsafe_allow_html=True)
    with ac4:
        if st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
            products = export_to_make_format(filtered, section_type)
            # إصلاح: اختيار الدالة الصحيحة حسب نوع القسم
            if section_type == "update":
                res = send_price_updates(products)
            elif section_type in ("missing", "new"):
                res = send_new_products(products)
            else:
                res = send_price_updates(products)
            if res["success"]:
                st.success(res["message"])
            else:
                st.error(res["message"])
    with ac5:
        # جمع القرارات المعلقة وإرسالها
        pending = {k: v for k, v in st.session_state.decisions_pending.items()
                   if v["action"] in ["approved", "deferred", "removed"]}
        if pending and st.button(f"📦 ترحيل {len(pending)} قرار → Make", key=f"{prefix}_send_decisions"):
            to_send = [{"name": k, "action": v["action"], "reason": v.get("reason", "")}
                       for k, v in pending.items()]
            res = send_price_updates(to_send)
            st.success(f"✅ تم إرسال {len(to_send)} قرار لـ Make")
            st.session_state.decisions_pending = {}

    st.caption(f"عرض {len(filtered)} من {len(df)} منتج — {datetime.now().strftime('%H:%M:%S')}")

    # ── Pagination ─────────────────────────────
    PAGE_SIZE = 25
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    if total_pages > 1:
        page_num = st.number_input("الصفحة", 1, total_pages, 1, key=f"{prefix}_pg")
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]

       # ── الجدول البصري ─────────────────────
    for idx, row in page_df.iterrows():
        our_name   = str(row.get("المنتج", "—"))
        # تخطي المنتجات التي أُرسلت لـ Make أو أُزيلت
        _hide_key = f"{prefix}_{our_name}_{idx}"
        if _hide_key in st.session_state.hidden_products:
            continue
        comp_name  = str(row.get("منتج_المنافس", "—"))
        our_price  = safe_float(row.get("السعر", 0))
        comp_price = safe_float(row.get("سعر_المنافس", 0))
        diff       = safe_float(row.get("الفرق", our_price - comp_price))
        match_pct  = safe_float(row.get("نسبة_التطابق", 0))
        comp_src   = str(row.get("المنافس", ""))
        brand      = str(row.get("الماركة", ""))
        size       = row.get("الحجم", "")
        ptype      = str(row.get("النوع", ""))
        risk       = str(row.get("الخطورة", ""))
        decision   = str(row.get("القرار", ""))
        ts_now     = datetime.now().strftime("%Y-%m-%d %H:%M")

        # سحب رقم المنتج من جميع الأعمدة المحتملة
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or ""
        )
        _pid_str = ""
        if _pid_raw and str(_pid_raw) not in ("", "nan", "None", "0"):
            try: _pid_str = str(int(float(str(_pid_raw))))
            except: _pid_str = str(_pid_raw)

        # بطاقة VS مع رقم المنتج
        st.markdown(vs_card(our_name, our_price, comp_name,
                            comp_price, diff, comp_src, _pid_str),
                    unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تاريخ آخر تغيير سعر
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            old_p = ph[1]["price"]
            chg = ph[0]["price"] - old_p
            chg_c = "#FF1744" if chg > 0 else "#00C853"
            price_change_html = f'<span style="color:{chg_c};font-size:.7rem">{"▲" if chg>0 else "▼"}{abs(chg):.0f} منذ {ph[1]["date"]}</span>'

        # قرار معلق؟
        pend = st.session_state.decisions_pending.get(our_name, {})
        pend_html = decision_badge(pend.get("action", "")) if pend else ""

        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:3px 12px;font-size:.8rem;flex-wrap:wrap;gap:4px;">
          <span>🏷️ <b>{brand}</b> {size} {ptype}</span>
          <span>تطابق: <b style="color:{match_color}">{match_pct:.0f}%</b></span>
          {risk_html}
          {price_change_html}
          {pend_html}
          {ts_badge(ts_now)}
        </div>""", unsafe_allow_html=True)

        # منافسين متعددين
        all_comps = row.get("جميع_المنافسين", row.get("جميع المنافسين", []))
        if isinstance(all_comps, list) and len(all_comps) > 1:
            with st.expander(f"👥 {len(all_comps)} منافس", expanded=False):
                for cm in all_comps:
                    st.markdown(
                        f'<div class="multi-comp">🏪 <b>{cm.get("competitor","")}</b>: '
                        f'{cm.get("name","")} — '
                        f'<span style="color:#ff9800">{cm.get("price",0):,.0f} ر.س</span> '
                        f'({cm.get("score",0):.0f}%)</div>',
                        unsafe_allow_html=True)

        # ── أزرار لكل منتج ─────────────────────
        b1, b2, b3, b4, b5, b6, b7, b8, b9 = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي حسب القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_{idx}"):
                with st.spinner("🤖 AI يحلل..."):
                    if prefix == "raise":
                        _ctx = (f"منتجنا «{our_name}» سعره {our_price:.0f} ر.س — المنافس «{comp_name}» ({comp_src}) سعره {comp_price:.0f} ر.س.\n"
                                f"سعرنا أعلى بـ {diff:.0f} ر.س.\n"
                                f"هل نخفض سعرنا؟ إذا نعم، ما السعر المقترح؟ إذا لا، لماذا؟")
                        r = call_ai(_ctx, "price_raise")
                        if r["success"]:
                            st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
                        else:
                            st.error("فشل AI")
                    elif prefix == "lower":
                        _ctx = (f"منتجنا «{our_name}» سعره {our_price:.0f} ر.س — المنافس «{comp_name}» ({comp_src}) سعره {comp_price:.0f} ر.س.\n"
                                f"سعرنا أقل بـ {abs(diff):.0f} ر.س = ربح ضائع.\n"
                                f"هل نرفع سعرنا؟ ما السعر الأمثل (أقل من المنافس بـ 5-15 ر.س)؟")
                        r = call_ai(_ctx, "price_lower")
                        if r["success"]:
                            st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
                        else:
                            st.error("فشل AI")
                    elif prefix == "review":
                        r = verify_match(our_name, comp_name, our_price, comp_price)
                        if r["success"]:
                            icon = "✅" if r.get("match") else "❌"
                            conf = r.get("confidence", 0)
                            reason = r.get("reason", "")[:200]
                            suggestion = r.get("suggestion", "")
                            st.info(f"{icon} **تطابق: {conf}%**\n\n{reason}")
                            if suggestion:
                                st.caption(f"💡 {suggestion}")
                        else:
                            st.error("فشل AI")
                    else:  # approved
                        r = verify_match(our_name, comp_name, our_price, comp_price)
                        if r["success"]:
                            icon = "✅" if r.get("match") else "❌"
                            st.info(f"{icon} {r.get('confidence',0)}% — {r.get('reason','')[:150]}")
                        else:
                            st.error("فشل AI")

        with b2:  # بحث سعر السوق ذكي
            _mkt_label = {"raise": "🌐 سعر عادل؟", "lower": "🌐 فرصة رفع؟"}.get(prefix, "🌐 سوق")
            if st.button(_mkt_label, key=f"mkt_{prefix}_{idx}"):
                with st.spinner("🌐 يبحث في السوق..."):
                    r = search_market_price(our_name, our_price)
                    if r.get("success"):
                        mp = r.get("market_price", 0)
                        rng = r.get("price_range", {})
                        rec = r.get("recommendation", "")
                        _verdict = ""
                        if prefix == "raise" and mp > 0:
                            _verdict = f"\n\n{'✅ سعرنا ضمن السوق' if our_price <= mp * 1.1 else '⚠️ سعرنا أعلى من السوق — يُنصح بالخفض'}"
                        elif prefix == "lower" and mp > 0:
                            _gap = mp - our_price
                            _verdict = f"\n\n{'💰 فرصة رفع ~' + f'{_gap:.0f} ر.س' if _gap > 10 else '✅ سعرنا قريب من السوق'}"
                        st.info(f"💹 سعر السوق: **{mp:,.0f} ر.س** ({rng.get('min',0):.0f}–{rng.get('max',0):.0f})\n\n{rec}{_verdict}")
                    else:
                        st.warning("تعذر البحث")

        with b3:  # موافق
            if st.button("✅ موافق", key=f"ok_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "approved", "reason": "موافقة يدوية",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "approved",
                             "موافقة يدوية", our_price, comp_price, diff, comp_src)
                st.success("✅")

        with b4:  # تأجيل
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        with b5:  # إزالة
            if st.button("🗑️ إزالة", key=f"rm_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "removed", "reason": "إزالة",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "removed",
                             "إزالة", our_price, comp_price, diff, comp_src)
                st.session_state.hidden_products.add(f"{prefix}_{our_name}_{idx}")
                st.rerun()

        with b6:  # سعر يدوي
            _auto_price = round(comp_price - 1, 2) if comp_price > 0 else our_price
            _custom_price = st.number_input(
                "سعر", value=_auto_price, min_value=0.0,
                step=1.0, key=f"cp_{prefix}_{idx}",
                label_visibility="collapsed"
            )

        with b7:  # تصدير Make
            if st.button("📤 Make", key=f"mk_{prefix}_{idx}"):
                # سحب رقم المنتج من جميع الأعمدة المحتملة
                _pid_raw = (
                    row.get("معرف_المنتج", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("معرف المنتج", "") or ""
                )
                # تحويل float إلى int (مثل 1081786650.0 → 1081786650)
                try:
                    _fv = float(_pid_raw)
                    _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                except (ValueError, TypeError):
                    _pid = str(_pid_raw).strip()
                if _pid in ("nan", "None", "NaN", ""): _pid = ""
                _final_price = _custom_price if _custom_price > 0 else _auto_price
                res = send_single_product({
                    "product_id": _pid,
                    "name": our_name, "price": _final_price,
                    "comp_name": comp_name, "comp_price": comp_price,
                    "diff": diff, "decision": decision, "competitor": comp_src
                })
                if res["success"]:
                    st.session_state.hidden_products.add(f"{prefix}_{our_name}_{idx}")
                    st.rerun()

        with b8:  # تحقق AI
            if st.button("🔍 تحقق", key=f"vrf_{prefix}_{idx}"):
                with st.spinner("🤖"):
                    _vr2 = verify_match(our_name, comp_name, our_price, comp_price)
                    if _vr2.get("success"):
                        _mc2 = "🟢 متطابق" if _vr2.get("match") else "🔴 غير متطابق"
                        st.markdown(f"{_mc2} — ثقة: **{_vr2.get('confidence',0)}%**")

        with b9:  # تاريخ السعر
            if st.button("📈 تاريخ", key=f"ph_{prefix}_{idx}"):
                history = get_price_history(our_name, comp_src)
                if history:
                    rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                    st.info("\n".join(rows_h))
                else:
                    st.info("لا يوجد تاريخ بعد")

        st.markdown('<hr style="border:none;border-top:1px solid #1a1a2e;margin:6px 0">', unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  الشريط الجانبي
# ════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"## {APP_ICON} {APP_TITLE}")
    st.caption(f"الإصدار {APP_VERSION}")

    # حالة AI — تشخيص مفصل
    ai_ok = bool(GEMINI_API_KEYS)
    if ai_ok:
        ai_color = "#00C853"
        ai_label = f"🤖 Gemini ✅ ({len(GEMINI_API_KEYS)} مفتاح)"
    else:
        ai_color = "#FF1744"
        ai_label = "🔴 AI غير متصل — تحقق من Secrets"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )

    # زر تشخيص سريع
    if not ai_ok:
        if st.button("🔍 تشخيص المشكلة", key="diag_btn"):
            import os
            st.write("**الـ secrets المتاحة:**")
            try:
                available = list(st.secrets.keys())
                for k in available:
                    val = str(st.secrets[k])
                    masked = val[:8] + "..." if len(val) > 8 else val
                    st.write(f"  `{k}` = `{masked}`")
            except Exception as e:
                st.error(f"خطأ: {e}")
            # محاولة مباشرة
            for key_name in ["GEMINI_API_KEYS","GEMINI_API_KEY","GEMINI_KEY_1"]:
                try:
                    v = st.secrets[key_name]
                    st.success(f"✅ وجدت {key_name} = {str(v)[:20]}...")
                except:
                    st.warning(f"❌ {key_name} غير موجود")

    # حالة المعالجة
    if st.session_state.job_id:
        job = get_job_progress(st.session_state.job_id)
        if job and job["status"] == "running":
            pct = job["processed"] / max(job["total"], 1)
            st.progress(pct, f"⚙️ {job['processed']}/{job['total']} منتج")

    page = st.radio("الأقسام", SECTIONS, label_visibility="collapsed")

    st.markdown("---")
    if st.session_state.results:
        r = st.session_state.results
        st.markdown("**📊 ملخص:**")
        for key, icon, label in [
            ("price_raise","🔴","أعلى"), ("price_lower","🟢","أقل"),
            ("approved","✅","موافق"), ("missing","🔍","مفقود"),
            ("review","⚠️","مراجعة")
        ]:
            cnt = len(r.get(key, pd.DataFrame()))
            st.caption(f"{icon} {label}: **{cnt}**")

    # قرارات معلقة
    pending_cnt = len(st.session_state.decisions_pending)
    if pending_cnt:
        st.markdown(f'<div style="background:#FF174422;border:1px solid #FF1744;'
                    f'border-radius:6px;padding:6px;text-align:center;color:#FF1744;'
                    f'font-size:.8rem">📦 {pending_cnt} قرار معلق</div>',
                    unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  1. لوحة التحكم
# ════════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    db_log("dashboard", "view")

    # تغييرات الأسعار
    changes = get_price_changes(7)
    if changes:
        st.markdown("#### 🔔 تغييرات أسعار آخر 7 أيام")
        c_df = pd.DataFrame(changes)
        st.dataframe(c_df[["product_name","competitor","old_price","new_price",
                            "price_diff","new_date"]].rename(columns={
            "product_name": "المنتج", "competitor": "المنافس",
            "old_price": "السعر السابق", "new_price": "السعر الجديد",
            "price_diff": "التغيير", "new_date": "التاريخ"
        }).head(200), use_container_width=True, height=200)
        st.markdown("---")

    if st.session_state.results:
        r = st.session_state.results
        cols = st.columns(5)
        data = [
            ("🔴","سعر أعلى",  len(r.get("price_raise", pd.DataFrame())), COLORS["raise"]),
            ("🟢","سعر أقل",   len(r.get("price_lower", pd.DataFrame())), COLORS["lower"]),
            ("✅","موافق",     len(r.get("approved", pd.DataFrame())),     COLORS["approved"]),
            ("🔍","مفقود",     len(r.get("missing", pd.DataFrame())),      COLORS["missing"]),
            ("⚠️","مراجعة",   len(r.get("review", pd.DataFrame())),       COLORS["review"]),
        ]
        for col, (icon, label, val, color) in zip(cols, data):
            col.markdown(stat_card(icon, label, val, color), unsafe_allow_html=True)

        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            sheets = {}
            for key, name in [("price_raise","سعر_أعلى"),("price_lower","سعر_أقل"),
                               ("approved","موافق"),("missing","مفقود"),("review","مراجعة")]:
                if key in r and not r[key].empty:
                    df_ex = r[key].copy()
                    if "جميع المنافسين" in df_ex.columns:
                        df_ex = df_ex.drop(columns=["جميع المنافسين"])
                    sheets[name] = df_ex
            if sheets:
                excel_all = export_multiple_sheets(sheets)
                st.download_button("📥 تصدير كل الأقسام Excel",
                    data=excel_all, file_name="mahwous_all.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with cc2:
            if st.button("📤 إرسال كل شيء لـ Make"):
                # إصلاح: إرسال تحديثات الأسعار (سعر أعلى وأقل)
                for key in ["price_raise","price_lower"]:
                    if key in r and not r[key].empty:
                        send_price_updates(export_to_make_format(r[key], "update"))
                # إرسال المنتجات المفقودة كمنتجات جديدة
                if "missing" in r and not r["missing"].empty:
                    send_new_products(export_to_make_format(r["missing"], "missing"))
                st.success("✅ تم إرسال جميع البيانات لـ Make!")
    else:
        # استئناف آخر job؟
        last = get_last_job()
        if last and last["status"] == "done" and last.get("results"):
            st.info(f"💾 يوجد تحليل محفوظ من {last.get('updated_at','')}")
            if st.button("🔄 استعادة النتائج المحفوظة"):
                df_all = pd.DataFrame(last["results"])
                if not df_all.empty:
                    st.session_state.results = {
                        "price_raise": df_all[df_all["القرار"].str.contains("أعلى",na=False)].reset_index(drop=True),
                        "price_lower": df_all[df_all["القرار"].str.contains("أقل", na=False)].reset_index(drop=True),
                        "approved":    df_all[df_all["القرار"].str.contains("موافق",na=False)].reset_index(drop=True),
                        "review":      df_all[df_all["القرار"].str.contains("مراجعة",na=False)].reset_index(drop=True),
                        "missing": pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame(), "all": df_all,
                    }
                    st.session_state.analysis_df = df_all
                    st.rerun()
        else:
            st.info("👈 ارفع ملفاتك من قسم 'رفع الملفات'")


# ════════════════════════════════════════════════
#  2. رفع الملفات
# ════════════════════════════════════════════════
elif page == "📂 رفع الملفات":
    st.header("📂 رفع الملفات")
    db_log("upload", "view")

    our_file   = st.file_uploader("📦 ملف منتجاتنا (CSV/Excel)",
                                   type=["csv","xlsx","xls"], key="our_file")
    comp_files = st.file_uploader("🏪 ملفات المنافسين (متعدد)",
                                   type=["csv","xlsx","xls"],
                                   accept_multiple_files=True, key="comp_files")

    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        bg_mode  = st.checkbox("⚡ معالجة خلفية (يمكنك التنقل أثناء التحليل)", value=True)
    with col_opt2:
        max_rows = st.number_input("حد الصفوف للمعالجة (0=كل)", 0, step=500)

    if st.button("🚀 بدء التحليل", type="primary"):
        if our_file and comp_files:
            our_df, err = read_file(our_file)
            if err:
                st.error(f"❌ {err}")
            else:
                if max_rows > 0:
                    our_df = our_df.head(int(max_rows))

                comp_dfs = {}
                for cf in comp_files:
                    cdf, cerr = read_file(cf)
                    if cerr: st.warning(f"⚠️ {cf.name}: {cerr}")
                    else: comp_dfs[cf.name] = cdf

                if comp_dfs:
                    # حفظ البيانات في session_state لاستخدامها لاحقاً في find_missing_products
                    st.session_state.our_df = our_df
                    st.session_state.comp_dfs = comp_dfs
                    job_id = str(uuid.uuid4())[:8]
                    st.session_state.job_id = job_id
                    comp_names = ",".join(comp_dfs.keys())

                    if bg_mode:
                        # ── خلفية ──
                        t = threading.Thread(
                            target=_run_analysis_background,
                            args=(job_id, our_df, comp_dfs,
                                  our_file.name, comp_names),
                            daemon=True
                        )
                        t.start()
                        st.session_state.job_running = True
                        st.success(f"✅ بدأ التحليل في الخلفية (Job: {job_id})")
                        st.info("🔄 تابع التقدم من لوحة التحكم أو انتظر هنا")

                        # polling
                        progress_bar = st.progress(0, "جاري التحليل...")
                        for _ in range(300):  # max 5 دقائق
                            time.sleep(2)
                            job = get_job_progress(job_id)
                            if job:
                                pct = job["processed"] / max(job["total"], 1)
                                progress_bar.progress(
                                    min(pct, 0.99),
                                    f"⚙️ {job['processed']}/{job['total']} منتج"
                                )
                                if job["status"] == "done":
                                    break
                                elif job["status"].startswith("error"):
                                    st.error(f"❌ {job['status']}")
                                    break

                        job = get_job_progress(job_id)
                        if job and job["status"] == "done" and job.get("results"):
                            df_all = pd.DataFrame(job["results"])
                            # استعادة المنتجات المفقودة من قاعدة البيانات
                            missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                            st.session_state.results = {
                                "price_raise": df_all[df_all["القرار"].str.contains("أعلى",na=False)].reset_index(drop=True),
                                "price_lower": df_all[df_all["القرار"].str.contains("أقل", na=False)].reset_index(drop=True),
                                "approved":    df_all[df_all["القرار"].str.contains("موافق",na=False)].reset_index(drop=True),
                                "review":      df_all[df_all["القرار"].str.contains("مراجعة",na=False)].reset_index(drop=True),
                                "missing": missing_df, "all": df_all,
                            }
                            st.session_state.analysis_df = df_all
                            progress_bar.progress(1.0, "✅ اكتمل!")
                            st.balloons()
                    else:
                        # ── مباشر ──
                        prog = st.progress(0, "جاري التحليل...")
                        def upd(p): prog.progress(p, f"{p*100:.0f}%")
                        df_all = run_full_analysis(our_df, comp_dfs, progress_callback=upd)
                        missing_df = find_missing_products(our_df, comp_dfs)

                        for _, row in df_all.iterrows():
                            if row.get("نسبة_التطابق", 0) > 0:
                                upsert_price_history(
                                    str(row.get("المنتج","")), str(row.get("المنافس","")),
                                    safe_float(row.get("سعر_المنافس",0)),
                                    safe_float(row.get("السعر",0)),
                                    safe_float(row.get("الفرق",0)),
                                    safe_float(row.get("نسبة_التطابق",0)),
                                    str(row.get("القرار",""))
                                )

                        st.session_state.results = {
                            "price_raise": df_all[df_all["القرار"].str.contains("أعلى",na=False)].reset_index(drop=True),
                            "price_lower": df_all[df_all["القرار"].str.contains("أقل", na=False)].reset_index(drop=True),
                            "approved":    df_all[df_all["القرار"].str.contains("موافق",na=False)].reset_index(drop=True),
                            "review":      df_all[df_all["القرار"].str.contains("مراجعة",na=False)].reset_index(drop=True),
                            "missing": missing_df, "all": df_all,
                        }
                        st.session_state.analysis_df = df_all
                        log_analysis(our_file.name, comp_names, len(our_df),
                                     len(df_all[df_all["نسبة_التطابق"]>0]), len(missing_df))
                        prog.progress(1.0, "✅ اكتمل!")
                        st.balloons()
        else:
            st.warning("⚠️ ارفع ملف منتجاتنا وملف منافس واحد على الأقل")


# ════════════════════════════════════════════════
#  3. سعر أعلى
# ════════════════════════════════════════════════
elif page == "🔴 سعر أعلى":
    st.header("🔴 منتجات سعرنا أعلى — فرصة خفض")
    db_log("price_raise", "view")
    if st.session_state.results and "price_raise" in st.session_state.results:
        df = st.session_state.results["price_raise"]
        if not df.empty:
            st.error(f"⚠️ {len(df)} منتج سعرنا أعلى من المنافسين")
            # AI تدريب لهذا القسم
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 احصل على تحليل شامل للقسم", key="ai_section_raise"):
                    with st.spinner("🤖 AI يحلل البيانات الفعلية..."):
                        _top = df.nlargest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق +{safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _avg_diff = safe_float(df["الفرق"].mean()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أعلى من المنافسين.\n"
                                   f"متوسط الفرق: {_avg_diff:.0f} ر.س\n"
                                   f"أعلى 15 فرق:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يجب خفض سعرها فوراً (فرق>30)؟\n"
                                   f"2. أي المنتجات يمكن إبقاؤها (فرق<10)؟\n"
                                   f"3. استراتيجية تسعير مخصصة لكل ماركة")
                        r = call_ai(_prompt, "price_raise")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "raise", "raise")
        else:
            st.success("✅ ممتاز! لا توجد منتجات بسعر أعلى")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  4. سعر أقل
# ════════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    st.header("🟢 منتجات سعرنا أقل — فرصة رفع")
    db_log("price_lower", "view")
    if st.session_state.results and "price_lower" in st.session_state.results:
        df = st.session_state.results["price_lower"]
        if not df.empty:
            st.info(f"💰 {len(df)} منتج يمكن رفع سعره لزيادة الهامش")
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 استراتيجية رفع الأسعار", key="ai_section_lower"):
                    with st.spinner("🤖 AI يحلل فرص الربح..."):
                        _top = df.nsmallest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق {safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _total_lost = safe_float(df["الفرق"].sum()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أقل من المنافسين.\n"
                                   f"إجمالي الأرباح الضائعة: {abs(_total_lost):.0f} ر.س\n"
                                   f"أكبر 15 فرصة ربح:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يمكن رفع سعرها فوراً (فرق>50)؟\n"
                                   f"2. أي المنتجات نرفعها تدريجياً (فرق 10-50)؟\n"
                                   f"3. كم الربح المتوقع إذا رفعنا الأسعار؟")
                        r = call_ai(_prompt, "price_lower")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "lower", "lower")
        else:
            st.info("لا توجد منتجات")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  5. موافق عليها
# ════════════════════════════════════════════════
elif page == "✅ موافق عليها":
    st.header("✅ منتجات موافق عليها")
    db_log("approved", "view")
    if st.session_state.results and "approved" in st.session_state.results:
        df = st.session_state.results["approved"]
        if not df.empty:
            st.success(f"✅ {len(df)} منتج بأسعار تنافسية مناسبة")
            render_pro_table(df, "approved", "approved")
        else:
            st.info("لا توجد منتجات موافق عليها")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  6. منتجات مفقودة
# ════════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات المنافسين غير الموجودة عندنا")
    db_log("missing", "view")

    if st.session_state.results and "missing" in st.session_state.results:
        df = st.session_state.results["missing"]
        if not df.empty:
            st.warning(f"⚠️ {len(df)} منتج مفقود")

            # AI للقسم
            with st.expander("🤖 نصيحة AI — أولويات الإضافة", expanded=False):
                if st.button("📡 تحليل المنتجات المفقودة", key="ai_missing_section"):
                    with st.spinner("🤖 AI يحلل أولويات الإضافة..."):
                        _sample = df.head(20)
                        _brands = df["الماركة"].value_counts().head(10).to_dict() if "الماركة" in df.columns else {}
                        _brand_summary = " | ".join(f"{b}: {c}" for b, c in _brands.items()) if _brands else "غير محدد"
                        _lines = "\n".join(
                            f"- {r.get('منتج_المنافس','')}: {safe_float(r.get('سعر_المنافس',0)):.0f} ر.س ({r.get('الماركة','')}) — عند {r.get('المنافس','')}"
                            for _, r in _sample.iterrows())
                        _prompt = (f"عندي {len(df)} منتج عند المنافسين غير موجود في متجرنا مهووس.\n"
                                   f"توزيع الماركات: {_brand_summary}\n"
                                   f"عينة من المنتجات:\n{_lines}\n\n"
                                   f"أعطني:\n1. ترتيب أولويات الإضافة (عالية/متوسطة/منخفضة) مع السبب\n"
                                   f"2. أي الماركات الأكثر أهمية للإضافة؟\n"
                                   f"3. سعر مقترح لكل منتج (أقل من المنافس بـ 5-10 ر.س)\n"
                                   f"4. هل هناك منتجات لا تستحق الإضافة؟")
                        r = call_ai(_prompt, "missing")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)

            # فلاتر
            opts = get_filter_options(df)
            with st.expander("🔍 فلاتر", expanded=False):
                c1, c2, c3 = st.columns(3)
                search  = c1.text_input("🔎 بحث", key="miss_s")
                brand_f = c2.selectbox("الماركة", opts["brands"], key="miss_b")
                comp_f  = c3.selectbox("المنافس", opts["competitors"], key="miss_c")

            filtered = df.copy()
            if search:
                filtered = filtered[filtered.apply(
                    lambda r: search.lower() in str(r.values).lower(), axis=1)]
            if brand_f != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(brand_f, case=False, na=False)]
            if comp_f != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(comp_f, case=False, na=False)]

            # تصدير
            cc1, cc2, cc3 = st.columns(3)
            with cc1:
                excel_m = export_to_excel(filtered, "مفقودة")
                st.download_button("📥 Excel", data=excel_m, file_name="missing.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="miss_dl")
            with cc2:
                _cmdf = filtered.copy()
                _csv_m = _cmdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button("📄 CSV", data=_csv_m, file_name="missing.csv",
                    mime="text/csv", key="miss_csv")
            with cc3:
                if st.button("📤 إرسال كل لـ Make", key="miss_make_all"):
                    products = export_to_make_format(filtered, "missing")
                    res = send_missing_products(products)
                    if res["success"]:
                        st.success(res["message"])
                    else:
                        st.error(res["message"])

            # تحميل المنتجات المخفية من قاعدة البيانات
            _hidden_db = get_hidden_product_keys("missing")

            # تصفية المنتجات المخفية (من الجلسة + من قاعدة البيانات)
            _visible_rows = []
            for _fi, _fr in filtered.iterrows():
                _fn = str(_fr.get("منتج_المنافس", ""))
                _fc = str(_fr.get("المنافس", ""))
                _hide_key = f"missing_{_fn}_{_fi}"
                if _hide_key in st.session_state.hidden_products:
                    continue
                if (_fn, _fc) in _hidden_db:
                    continue
                _visible_rows.append(_fi)
            filtered = filtered.loc[_visible_rows]

            st.caption(f"{len(filtered)} منتج — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            # عرض كل منتج
            PAGE_SIZE = 20
            total_p = len(filtered)
            tp = max(1, (total_p + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="miss_pg") if tp > 1 else 1
            page_df = filtered.iloc[(pn-1)*PAGE_SIZE:pn*PAGE_SIZE]

            for idx, row in page_df.iterrows():
                name   = str(row.get("منتج_المنافس", ""))
                price  = safe_float(row.get("سعر_المنافس", 0))
                brand  = str(row.get("الماركة", ""))
                comp   = str(row.get("المنافس", ""))
                size   = row.get("الحجم", "")
                ptype  = str(row.get("النوع", ""))

                st.markdown(f"""
                <div style="border:1px solid #007bff44;border-radius:8px;padding:12px;
                            margin:4px 0;background:linear-gradient(90deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;align-items:center">
                    <div style="flex:1">
                      <div style="font-weight:700;color:#4fc3f7;font-size:.95rem">{name}</div>
                      <div style="font-size:.75rem;color:#888;margin-top:3px">
                        🏷️ {brand} | 📏 {size} | 🧴 {ptype} | 🏪 {comp}
                      </div>
                    </div>
                    <div style="font-size:1.2rem;font-weight:900;color:#ff9800;margin-left:12px">
                      {price:,.0f} ر.س
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                b1, b2, b3, b4, b5, b6, b7 = st.columns(7)

                with b1:  # صورة + مكونات
                    if st.button("🖼️ صورة", key=f"img_{idx}"):
                        with st.spinner("يجلب من Fragrantica Arabia..."):
                            fi = fetch_fragrantica_info(name)
                            if fi.get("success"):
                                img = fi.get("image_url","")
                                if img and img.startswith("http"):
                                    st.image(img, width=180, caption=name)
                                top = ", ".join(fi.get("top_notes",[]))
                                mid = ", ".join(fi.get("middle_notes",[]))
                                base = ", ".join(fi.get("base_notes",[]))
                                if top or mid or base:
                                    st.markdown(f"🌸 **قمة:** {top}  \n💐 **قلب:** {mid}  \n🌿 **قاعدة:** {base}")
                                if fi.get("description_ar"):
                                    st.info(fi["description_ar"][:200])
                                if fi.get("fragrantica_url"):
                                    st.markdown(f"[🔗 Fragrantica Arabia]({fi['fragrantica_url']})")
                            else:
                                st.warning("لم يتم العثور على صورة")

                with b2:  # وصف مهووس
                    if st.button("✍️ وصف مهووس", key=f"mhdesc_{idx}"):
                        with st.spinner("يولّد الوصف..."):
                            fi2 = fetch_fragrantica_info(name)
                            desc = generate_mahwous_description(name, price, fi2)
                            st.text_area("وصف المنتج — نسخ للمتجر:", desc, height=250, key=f"mhd_ta_{idx}")

                with b3:  # تحقق تكرار AI
                    if st.button("🤖 تكرار؟", key=f"dup_{idx}"):
                        with st.spinner("..."):
                            our_prods = []
                            if st.session_state.analysis_df is not None:
                                our_prods = st.session_state.analysis_df.get(
                                    "المنتج", pd.Series()).tolist()
                            r = check_duplicate(name, our_prods[:50])
                            st.info(r["response"][:200] if r["success"] else "فشل")

                with b4:  # بحث في مهووس
                    if st.button("🔎 مهووس", key=f"mhw_{idx}"):
                        with st.spinner("يبحث في mahwous.com..."):
                            r = search_mahwous(name)
                            if r.get("success"):
                                avail = "✅ متوفر" if r.get("likely_available") else "❌ غير متوفر"
                                pri = r.get("add_recommendation", "")
                                reason = r.get("reason", "")[:150]
                                sp = r.get("suggested_price", 0)
                                st.info(f"{avail} | أولوية: **{pri}** | سعر مقترح: {sp:,.0f}ر.س\n\n{reason}")
                            else:
                                st.warning("تعذر البحث")

                with b5:  # بحث سعر السوق
                    if st.button("💹 سوق", key=f"mkt_m_{idx}"):
                        with st.spinner("🌐 يبحث في السوق..."):
                            r = search_market_price(name, price)
                            if r.get("success"):
                                mp = r.get("market_price", 0)
                                rng = r.get("price_range", {})
                                rec = r.get("recommendation", "")[:200]
                                mn = rng.get("min",0); mx = rng.get("max",0)
                                st.markdown(f"""
<div style="background:#0e1a2e;border:1px solid #007bff44;border-radius:8px;padding:10px;">
  <div style="font-weight:700;color:#4fc3f7">💹 سعر السوق: {mp:,.0f} ر.س</div>
  <div style="color:#888;font-size:.8rem">النطاق: {mn:,.0f} - {mx:,.0f} ر.س</div>
  <div style="color:#aaa;font-size:.82rem;margin-top:6px">{rec}</div>
</div>""", unsafe_allow_html=True)

                with b6:  # إضافة للـ Make
                    if st.button("📤 Make", key=f"mk_m_{idx}"):
                        _size_val = extract_size(name)
                        _size_str = f"{int(_size_val)}ml" if _size_val else str(size)
                        _suggested_price = round(price - 1, 2) if price > 0 else 0
                        res = send_new_products([{
                            "أسم المنتج": name,
                            "سعر المنتج": _suggested_price,
                            "brand": brand,
                            "الوصف": f"عطر {brand} {_size_str}" if brand else f"عطر {_size_str}",
                        }])
                        if res["success"]:
                            save_hidden_product(name, comp, "sent_to_make", price, "missing")
                            log_decision(name, "missing", "sent_to_make", "إرسال لـ Make", 0, price, -price, comp)
                            st.session_state.hidden_products.add(f"missing_{name}_{idx}")
                            st.rerun()
                        else:
                            st.error(res["message"])

                with b7:  # حذف نهائي
                    if st.button("🗑️ حذف", key=f"ign_{idx}"):
                        save_hidden_product(name, comp, "deleted", price, "missing")
                        log_decision(name, "missing", "deleted", "حذف نهائي", 0, price, -price, comp)
                        st.session_state.hidden_products.add(f"missing_{name}_{idx}")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #111;margin:4px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات مفقودة!")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  7. تحت المراجعة
# ════════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    st.header("⚠️ منتجات تحت المراجعة")
    db_log("review", "view")
    if st.session_state.results and "review" in st.session_state.results:
        df = st.session_state.results["review"]
        if not df.empty:
            st.warning(f"⚠️ {len(df)} منتج بتطابق غير مؤكد")
            with st.expander("🤖 نصيحة AI — كيف تتعامل مع المراجعة", expanded=False):
                if st.button("📡 تحليل قسم المراجعة", key="ai_review_section"):
                    with st.spinner("🤖 AI يراجع المطابقات المشكوك فيها..."):
                        _sample = df.head(15)
                        _lines = "\n".join(
                            f"- منتجنا: «{r.get('المنتج','')}» ↔ المنافس: «{r.get('منتج_المنافس','')}» | تطابق: {safe_float(r.get('نسبة_التطابق',0)):.0f}% | سعرنا: {safe_float(r.get('السعر',0)):.0f} | المنافس: {safe_float(r.get('سعر_المنافس',0)):.0f}"
                            for _, r in _sample.iterrows())
                        _avg_match = safe_float(df["نسبة_التطابق"].mean()) if "نسبة_التطابق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج بتطابق غير مؤكد (متوسط التطابق {_avg_match:.0f}%).\n"
                                   f"المنتجات:\n{_lines}\n\n"
                                   f"لكل منتج قرّر:\n"
                                   f"✅ متطابق (نفس المنتج بأسماء مختلفة)\n"
                                   f"❌ غير متطابق (منتجات مختلفة)\n"
                                   f"⚠️ يحتاج تحقق يدوي\n"
                                   f"اشرح سبب قرارك لكل منتج.")
                        r = call_ai(_prompt, "review")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "review", "update")
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  8. الذكاء الاصطناعي — Gemini مباشر
# ════════════════════════════════════════════════
elif page == "🤖 الذكاء الصناعي":
    db_log("ai", "view")

    # ── شريط الحالة ──
    if GEMINI_API_KEYS:
        st.markdown(f'''<div style="background:linear-gradient(90deg,#051505,#030d1f);
            border:1px solid #00C853;border-radius:10px;padding:10px 18px;
            margin-bottom:12px;display:flex;align-items:center;gap:10px;">
          <div style="width:10px;height:10px;border-radius:50%;background:#00C853;
                      box-shadow:0 0 8px #00C853;animation:pulse 2s infinite"></div>
          <span style="color:#00C853;font-weight:800;font-size:1rem">Gemini Flash — متصل مباشرة</span>
          <span style="color:#555;font-size:.78rem"> | {len(GEMINI_API_KEYS)} مفاتيح | {GEMINI_MODEL}</span>
        </div>''', unsafe_allow_html=True)
    else:
        st.error("❌ Gemini غير متصل — أضف GEMINI_API_KEYS في Streamlit Secrets")

    # ── سياق البيانات ──
    _ctx = []
    if st.session_state.results:
        _r = st.session_state.results
        _ctx = [
            f"المنتجات الكلية: {len(_r.get('all', pd.DataFrame()))}",
            f"سعر أعلى: {len(_r.get('price_raise', pd.DataFrame()))}",
            f"سعر أقل: {len(_r.get('price_lower', pd.DataFrame()))}",
            f"موافق: {len(_r.get('approved', pd.DataFrame()))}",
            f"مراجعة: {len(_r.get('review', pd.DataFrame()))}",
            f"مفقود: {len(_r.get('missing', pd.DataFrame()))}",
        ]
    _ctx_str = " | ".join(_ctx) if _ctx else "لم يتم تحليل بيانات بعد"

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "💬 دردشة مباشرة", "📋 لصق وتحليل", "🔍 تحقق منتج", "💹 بحث سوق", "📊 أوامر مجمعة"
    ])

    # ═══ TAB 1: دردشة Gemini مباشرة ═══════════
    with tab1:
        st.caption(f"📊 البيانات: {_ctx_str}")

        # صندوق المحادثة
        _chat_h = 430
        _msgs_html = ""
        if not st.session_state.chat_history:
            _msgs_html = """<div style="text-align:center;padding:60px 20px;color:#333">
              <div style="font-size:3rem">🤖</div>
              <div style="color:#666;margin-top:10px;font-size:1rem">Gemini Flash جاهز للمساعدة</div>
              <div style="color:#444;margin-top:6px;font-size:.82rem">
                اسأل عن الأسعار · المنتجات · توصيات التسعير · تحليل المنافسين
              </div>
            </div>"""
        else:
            for h in st.session_state.chat_history[-15:]:
                _msgs_html += f"""
                <div style="display:flex;justify-content:flex-end;margin:5px 0">
                  <div style="background:#1e1e3f;color:#B8B4FF;padding:8px 14px;
                              border-radius:14px 14px 2px 14px;max-width:82%;font-size:.88rem;
                              line-height:1.5">{h['user']}</div>
                </div>
                <div style="display:flex;justify-content:flex-start;margin:4px 0 10px 0">
                  <div style="background:#080f1e;border:1px solid #1a3050;color:#d0d0d0;
                              padding:10px 14px;border-radius:14px 14px 14px 2px;
                              max-width:88%;font-size:.88rem;line-height:1.65">
                    <span style="color:#00C853;font-size:.65rem;font-weight:700">
                      ● {h.get('source','Gemini')} · {h.get('ts','')}</span><br>
                    {h['ai'].replace(chr(10),'<br>')}
                  </div>
                </div>"""

        st.markdown(
            f'''<div style="background:#050b14;border:1px solid #1a3050;border-radius:12px;
                padding:14px;height:{_chat_h}px;overflow-y:auto;direction:rtl">
              {_msgs_html}
            </div>''', unsafe_allow_html=True)

        # إدخال
        _mc1, _mc2 = st.columns([5, 1])
        with _mc1:
            _user_in = st.text_input("", key="gem_in",
                placeholder="اسأل Gemini — عن المنتجات، الأسعار، التوصيات...",
                label_visibility="collapsed")
        with _mc2:
            _send = st.button("➤ إرسال", key="gem_send", type="primary", use_container_width=True)

        # أزرار سريعة
        _qc = st.columns(4)
        _quick = None
        _quick_labels = [
            ("📉 أولويات الخفض", "بناءً على البيانات المحملة أعطني أولويات خفض الأسعار مع الأرقام"),
            ("📈 فرص الرفع", "حلّل فرص رفع الأسعار وأعطني توصية مرتبة"),
            ("🔍 أولويات المفقودات", "حلّل المنتجات المفقودة وأعطني أولويات الإضافة"),
            ("📊 ملخص شامل", f"أعطني ملخصاً تنفيذياً: {_ctx_str}"),
        ]
        for i, (lbl, q) in enumerate(_quick_labels):
            with _qc[i]:
                if st.button(lbl, key=f"q{i}", use_container_width=True):
                    _quick = q

        _msg_to_send = _quick or (_user_in if _send and _user_in else None)
        if _msg_to_send:
            _full = f"سياق البيانات: {_ctx_str}\n\n{_msg_to_send}"
            with st.spinner("🤖 Gemini يفكر..."):
                _res = gemini_chat(_full, st.session_state.chat_history)
            if _res["success"]:
                st.session_state.chat_history.append({
                    "user": _msg_to_send, "ai": _res["response"],
                    "source": _res.get("source","Gemini"),
                    "ts": datetime.now().strftime("%H:%M")
                })
                st.rerun()
            else:
                st.error(_res["response"])

        _dc1, _dc2 = st.columns([4,1])
        with _dc2:
            if st.session_state.chat_history:
                if st.button("🗑️ مسح", key="clr_chat"):
                    st.session_state.chat_history = []
                    st.rerun()

    # ═══ TAB 2: لصق وتحليل ══════════════════════
    with tab2:
        st.markdown("**الصق منتجات أو بيانات أو أوامر — Gemini سيحللها فوراً:**")

        _paste = st.text_area(
            "الصق هنا:",
            height=200, key="paste_box",
            placeholder="""يمكنك لصق:
• قائمة منتجات من Excel (Ctrl+C ثم Ctrl+V)
• أوامر: "خفّض كل منتج فرقه أكثر من 30 ريال"
• CSV مباشرة
• أي نص تريد تحليله""")

        _pc1, _pc2 = st.columns(2)
        with _pc1:
            if st.button("🤖 تحليل بـ Gemini", key="paste_go", type="primary", use_container_width=True):
                if _paste:
                    # إضافة سياق البيانات الحالية
                    _ctx_data = ""
                    if st.session_state.results:
                        _r2 = st.session_state.results
                        _all = _r2.get("all", pd.DataFrame())
                        if not _all.empty and len(_all) > 0:
                            cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار"] if c in _all.columns]
                            if cols:
                                _ctx_data = "\n\nعينة من بيانات التطبيق:\n" + _all[cols].head(15).to_string(index=False)
                    with st.spinner("🤖 Gemini يحلل..."):
                        _pr = analyze_paste(_paste, _ctx_data)
                    st.markdown(f'<div class="ai-box">{_pr["response"]}</div>', unsafe_allow_html=True)
        with _pc2:
            if st.button("📊 تحويل لجدول", key="paste_table", use_container_width=True):
                if _paste:
                    try:
                        import io as _io
                        _df_p = pd.read_csv(_io.StringIO(_paste), sep=None, engine='python')
                        st.dataframe(_df_p.head(200), use_container_width=True)
                        _csv_p = _df_p.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                        st.download_button("📄 تحميل CSV", data=_csv_p,
                            file_name="pasted.csv", mime="text/csv", key="paste_dl")
                    except:
                        st.warning("تعذر التحويل لجدول — جرب تنسيق CSV أو TSV")

    # ═══ TAB 3: تحقق منتج ══════════════════════
    with tab3:
        st.markdown("**تحقق من تطابق منتجين بدقة 100%:**")
        _vc1, _vc2 = st.columns(2)
        _vp1 = _vc1.text_input("🏷️ منتجنا:", key="v_our", placeholder="Dior Sauvage EDP 100ml")
        _vp2 = _vc2.text_input("🏪 المنافس:", key="v_comp", placeholder="ديور سوفاج بارفان 100 مل")
        _vc3, _vc4 = st.columns(2)
        _vpr1 = _vc3.number_input("💰 سعرنا:", 0.0, key="v_p1")
        _vpr2 = _vc4.number_input("💰 سعر المنافس:", 0.0, key="v_p2")
        if st.button("🔍 تحقق الآن", key="vbtn", type="primary"):
            if _vp1 and _vp2:
                with st.spinner("🤖 AI يتحقق..."):
                    _vr = verify_match(_vp1, _vp2, _vpr1, _vpr2)
                if _vr["success"]:
                    _mc = "#00C853" if _vr.get("match") else "#FF1744"
                    _ml = "✅ متطابقان" if _vr.get("match") else "❌ غير متطابقان"
                    st.markdown(f'''<div style="background:{_mc}22;border:1px solid {_mc};
                        border-radius:8px;padding:12px;margin:8px 0">
                      <div style="color:{_mc};font-weight:800;font-size:1.1rem">{_ml}</div>
                      <div style="color:#aaa;margin-top:4px">ثقة: <b>{_vr.get("confidence",0)}%</b></div>
                      <div style="color:#888;font-size:.88rem;margin-top:6px">{_vr.get("reason","")}</div>
                    </div>''', unsafe_allow_html=True)
                    if _vr.get("suggestion"):
                        st.info(f"💡 {_vr['suggestion']}")
                else:
                    st.error("فشل الاتصال")

    # ═══ TAB 4: بحث السوق ══════════════════════
    with tab4:
        st.markdown("**ابحث عن سعر السوق الحقيقي لأي منتج:**")
        _ms1, _ms2 = st.columns([3,1])
        with _ms1:
            _mprod = st.text_input("🔎 اسم المنتج:", key="mkt_prod",
                                    placeholder="Dior Sauvage EDP 100ml")
        with _ms2:
            _mcur = st.number_input("💰 سعرنا:", 0.0, key="mkt_price")

        if st.button("🌐 ابحث في السوق", key="mkt_btn", type="primary"):
            if _mprod:
                with st.spinner("🌐 Gemini يبحث في السوق..."):
                    _mr = search_market_price(_mprod, _mcur)
                if _mr.get("success"):
                    _mp = _mr.get("market_price", 0)
                    _rng = _mr.get("price_range", {})
                    _comps = _mr.get("competitors", [])
                    _rec = _mr.get("recommendation","")
                    _diff_v = _mp - _mcur if _mcur > 0 else 0
                    _diff_c = "#00C853" if _diff_v > 0 else "#FF1744" if _diff_v < 0 else "#888"

                    _src1, _src2 = st.columns(2)
                    with _src1:
                        st.metric("💹 سعر السوق", f"{_mp:,.0f} ر.س",
                                  delta=f"{_diff_v:+.0f} ر.س" if _mcur > 0 else None)
                    with _src2:
                        _mn = _rng.get("min",0); _mx = _rng.get("max",0)
                        st.metric("📊 نطاق السعر", f"{_mn:,.0f} - {_mx:,.0f} ر.س")

                    if _comps:
                        st.markdown("**🏪 منافسون في السوق:**")
                        for _c in _comps[:5]:
                            _cpv = float(_c.get("price",0))
                            _dv = _cpv - _mcur if _mcur > 0 else 0
                            st.markdown(
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س "
                                f"({'أعلى' if _dv>0 else 'أقل'} بـ {abs(_dv):.0f}ر.س)" if _dv != 0 else
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س"
                            )
                    if _rec:
                        st.markdown(f'<div class="ai-box">💡 {_rec}</div>', unsafe_allow_html=True)

        # صورة المنتج من Fragrantica
        with st.expander("🖼️ صورة ومكونات من Fragrantica Arabia", expanded=False):
            _fprod = st.text_input("اسم العطر:", key="frag_prod",
                                    placeholder="Dior Sauvage EDP")
            if st.button("🔍 ابحث في Fragrantica", key="frag_btn"):
                if _fprod:
                    with st.spinner("يجلب من Fragrantica Arabia..."):
                        _fi = fetch_fragrantica_info(_fprod)
                    if _fi.get("success"):
                        _fic1, _fic2 = st.columns([1,2])
                        with _fic1:
                            _img_url = _fi.get("image_url","")
                            if _img_url and _img_url.startswith("http"):
                                st.image(_img_url, width=200, caption=_fprod)
                            else:
                                st.markdown(f"[🔗 Fragrantica Arabia]({_FR}/search/?query={_fprod.replace(' ','+')})")
                        with _fic2:
                            _top = ", ".join(_fi.get("top_notes",[])[:5])
                            _mid = ", ".join(_fi.get("middle_notes",[])[:5])
                            _base = ", ".join(_fi.get("base_notes",[])[:5])
                            st.markdown(f"""
🌸 **القمة:** {_top or "—"}
💐 **القلب:** {_mid or "—"}
🌿 **القاعدة:** {_base or "—"}
📝 **{_fi.get('description_ar','')}**""")
                        if _fi.get("fragrantica_url"):
                            st.markdown(f"[🌐 صفحة العطر في Fragrantica]({_fi['fragrantica_url']})")
                    else:
                        st.info("لم يتم العثور على بيانات — تحقق من اسم العطر")

    # ═══ TAB 5: أوامر مجمعة ════════════════════
    with tab5:
        st.markdown("**نفّذ أوامر مجمعة على بياناتك:**")
        st.caption(f"📊 البيانات: {_ctx_str}")

        _cmd_section = st.selectbox(
            "اختر القسم:", ["الكل", "سعر أعلى", "سعر أقل", "موافق", "مراجعة", "مفقود"],
            key="cmd_sec"
        )
        _cmd_text = st.text_area(
            "الأمر أو السؤال:", height=120, key="cmd_area",
            placeholder="""أمثلة:
• حلّل المنتجات التي فرقها أكثر من 30 ريال وأعطني توصية
• رتّب المنتجات حسب الأولوية
• ما المنتجات التي تحتاج خفض سعر فوري؟
• أعطني ملخص مقارنة مع المنافسين"""
        )

        if st.button("⚡ تنفيذ الأمر", key="cmd_run", type="primary"):
            if _cmd_text and st.session_state.results:
                _sec_map = {
                    "سعر أعلى":"price_raise","سعر أقل":"price_lower",
                    "موافق":"approved","مراجعة":"review","مفقود":"missing"
                }
                _df_sec = None
                if _cmd_section != "الكل":
                    _k = _sec_map.get(_cmd_section)
                    _df_sec = st.session_state.results.get(_k, pd.DataFrame())
                else:
                    _df_sec = st.session_state.results.get("all", pd.DataFrame())

                if _df_sec is not None and not _df_sec.empty:
                    _cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار","الفرق"] if c in _df_sec.columns]
                    _sample = _df_sec[_cols].head(25).to_string(index=False) if _cols else ""
                    _full_cmd = f"""البيانات ({_cmd_section}) - {len(_df_sec)} منتج:
{_sample}

الأمر: {_cmd_text}"""
                    with st.spinner("⚡ Gemini ينفذ الأمر..."):
                        _cr = call_ai(_full_cmd, "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
                else:
                    with st.spinner("🤖"):
                        _cr = call_ai(f"{_ctx_str}\n\n{_cmd_text}", "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
            elif _cmd_text:
                with st.spinner("🤖"):
                    _cr = call_ai(_cmd_text, "general")
                st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  9. أتمتة Make
# ════════════════════════════════════════════════
elif page == "⚡ أتمتة Make":
    st.header("⚡ أتمتة Make.com")
    db_log("make", "view")

    tab1, tab2, tab3 = st.tabs(["🔗 حالة الاتصال", "📤 إرسال", "📦 القرارات المعلقة"])

    with tab1:
        if st.button("🔍 فحص الاتصال"):
            with st.spinner("..."):
                results = verify_webhook_connection()
                for name, r in results.items():
                    if name != "all_connected":
                        color = "🟢" if r["success"] else "🔴"
                        st.markdown(f"{color} **{name}:** {r['message']}")
                if results.get("all_connected"):
                    st.success("✅ جميع الاتصالات تعمل")

    with tab2:
        if st.session_state.results:
            wh = st.selectbox("نوع الإرسال", ["تحديث أسعار","منتجات جديدة","مفقودة"])
            key_map = {"تحديث أسعار":"price_raise","منتجات جديدة":"price_lower","مفقودة":"missing"}
            # إصلاح: ربط نوع الإرسال بـ section_type الصحيح لـ export_to_make_format
            section_type_map = {"price_raise":"update","price_lower":"new","missing":"missing"}
            sec_key = key_map[wh]
            df_s = st.session_state.results.get(sec_key, pd.DataFrame())
            if not df_s.empty:
                st.info(f"سيتم إرسال {len(df_s)} منتج")
                if st.button("📤 إرسال الآن"):
                    sec_type = section_type_map[sec_key]
                    products = export_to_make_format(df_s, sec_type)
                    func = {"تحديث أسعار": send_price_updates,
                            "منتجات جديدة": send_new_products,
                            "مفقودة": send_missing_products}
                    res = func[wh](products)
                    if res["success"]:
                        st.success(res["message"])
                    else:
                        st.error(res["message"])

    with tab3:
        pending = st.session_state.decisions_pending
        if pending:
            st.info(f"📦 {len(pending)} قرار معلق")
            df_p = pd.DataFrame([
                {"المنتج": k, "القرار": v["action"],
                 "وقت القرار": v.get("ts",""), "المنافس": v.get("competitor","")}
                for k, v in pending.items()
            ])
            st.dataframe(df_p.head(200), use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                if st.button("📤 إرسال كل القرارات لـ Make"):
                    to_send = [{"name": k, **v} for k, v in pending.items()]
                    res = send_price_updates(to_send)
                    st.success(res["message"])
                    st.session_state.decisions_pending = {}
                    st.rerun()
            with c2:
                if st.button("🗑️ مسح القرارات"):
                    st.session_state.decisions_pending = {}
                    st.rerun()
        else:
            st.info("لا توجد قرارات معلقة")


# ════════════════════════════════════════════════
#  10. الإعدادات
# ════════════════════════════════════════════════
elif page == "⚙️ الإعدادات":
    st.header("⚙️ الإعدادات")
    db_log("settings", "view")

    tab1, tab2, tab3 = st.tabs(["🔑 المفاتيح", "⚙️ المطابقة", "📜 السجل"])

    with tab1:
        gemini_s = f"✅ {len(GEMINI_API_KEYS)} مفتاح" if GEMINI_API_KEYS else "❌"
        or_s = "✅ مفعل" if OPENROUTER_API_KEY else "❌"
        st.info(f"Gemini API: {gemini_s}")
        st.info(f"OpenRouter: {or_s}")
        st.info(f"Webhook أسعار: {'✅' if WEBHOOK_UPDATE_PRICES else '❌'}")
        st.info(f"Webhook منتجات: {'✅' if WEBHOOK_NEW_PRODUCTS else '❌'}")
        if st.button("🧪 اختبار AI"):
            with st.spinner("..."):
                r = call_ai("مرحباً، اختبار سريع. أجب: يعمل")
                if r["success"]:
                    st.success(f"✅ AI يعمل ({r['source']}): {r['response'][:80]}")
                else:
                    st.error(r["response"])

    with tab2:
        st.info(f"حد التطابق الأدنى: {MIN_MATCH_SCORE}%")
        st.info(f"حد التطابق العالي: {HIGH_MATCH_SCORE}%")
        st.info(f"هامش فرق السعر: {PRICE_DIFF_THRESHOLD} ر.س")

    with tab3:
        decisions = get_decisions(limit=30)
        if decisions:
            df_dec = pd.DataFrame(decisions)
            st.dataframe(df_dec[["timestamp","product_name","old_status",
                                  "new_status","reason","competitor"]].rename(columns={
                "timestamp":"التاريخ","product_name":"المنتج",
                "old_status":"من","new_status":"إلى",
                "reason":"السبب","competitor":"المنافس"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة")


# ════════════════════════════════════════════════
#  11. السجل
# ════════════════════════════════════════════════
elif page == "📜 السجل":
    st.header("📜 السجل الكامل")
    db_log("log", "view")

    tab1, tab2, tab3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

    with tab1:
        history = get_analysis_history(20)
        if history:
            df_h = pd.DataFrame(history)
            st.dataframe(df_h[["timestamp","our_file","comp_file",
                                "total_products","matched","missing"]].rename(columns={
                "timestamp":"التاريخ","our_file":"ملف منتجاتنا",
                "comp_file":"ملف المنافس","total_products":"الإجمالي",
                "matched":"متطابق","missing":"مفقود"
            }).head(200), use_container_width=True)
        else:
            st.info("لا يوجد تاريخ")

    with tab2:
        days = st.slider("آخر X يوم", 1, 30, 7)
        changes = get_price_changes(days)
        if changes:
            df_c = pd.DataFrame(changes)
            st.dataframe(df_c.rename(columns={
                "product_name":"المنتج","competitor":"المنافس",
                "old_price":"السعر السابق","new_price":"السعر الجديد",
                "price_diff":"التغيير","new_date":"تاريخ التغيير"
            }).head(200), use_container_width=True)
        else:
            st.info(f"لا توجد تغييرات في آخر {days} يوم")

    with tab3:
        events = get_events(limit=50)
        if events:
            df_e = pd.DataFrame(events)
            st.dataframe(df_e[["timestamp","page","event_type","details"]].rename(columns={
                "timestamp":"التاريخ","page":"الصفحة",
                "event_type":"الحدث","details":"التفاصيل"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد أحداث")
