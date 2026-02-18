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
        b1, b2, b3, b4, b5, b6, b7, b8 = st.columns(8)

        with b1:  # AI تحقق
            if st.button("🤖 تحقق", key=f"v_{prefix}_{idx}"):
                with st.spinner("AI..."):
                    r = verify_match(our_name, comp_name, our_price, comp_price)
                    if r["success"]:
                        icon = "✅" if r.get("match") else "❌"
                        st.info(f"{icon} {r.get('confidence',0)}% — {r.get('reason','')[:150]}")
                    else:
                        st.error("فشل AI")

        with b2:  # بحث سعر السوق
