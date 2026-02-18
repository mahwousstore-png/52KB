#!/usr/bin/env python3
"""اختبار شامل لمحرك المقارنة على ملفات المنافسين الفعلية"""
import sys, os, io, glob
sys.path.insert(0, os.getcwd())

import pandas as pd
from engines.engine import (normalize, extract_brand, extract_size, extract_type,
                             extract_gender, extract_product_line, CompIndex,
                             read_file, _fcol, _price, _smart_rename_columns)

class FakeFile:
    """محاكاة Streamlit UploadedFile"""
    def __init__(self, path):
        self._path = path
        self.name = os.path.basename(path)
        self._data = open(path, 'rb').read()
        self._buf = io.BytesIO(self._data)
    def read(self, *a): return self._buf.read(*a)
    def seek(self, *a): return self._buf.seek(*a)

# === Test 1: Read all CSV files ===
print("=" * 60)
print("  Test 1: Read CSV files")
print("=" * 60)
files = glob.glob("متجر*.csv") + glob.glob("متحر*.csv") + glob.glob("منتجات*.csv")
dfs = {}
for f in sorted(files):
    try:
        fobj = FakeFile(f)
        df, err = read_file(fobj)
        if err:
            print(f"❌ {f}: {err}")
            continue
        pcol = _fcol(df, ["السعر","سعر","Price","price","PRICE"])
        ncol = _fcol(df, ["المنتج","اسم المنتج","Product","Name","name"])
        sp = 0
        if pcol and len(df[pcol].dropna()) > 0:
            try: sp = float(str(df[pcol].dropna().iloc[0]).replace(',',''))
            except: pass
        sn = ""
        if ncol and len(df[ncol].dropna()) > 0:
            sn = str(df[ncol].dropna().iloc[0])[:40]
        print(f"✅ {f[:35]:35s}: {len(df):5d} | ncol={ncol} | pcol={pcol} | price={sp} | name={sn}")
        dfs[f] = df
    except Exception as e:
        print(f"❌ {f}: {e}")

# === Test 2: extract_product_line ===
print("\n" + "=" * 60)
print("  Test 2: extract_product_line")
print("=" * 60)
tests = [
    ("عطر بربري هيرو أو دو تواليت 100مل (للرجال)", "Burberry"),
    ("عطر لندن من بربري للرجال - او دي تواليت 100", "Burberry"),
    ("تستر ميسوني ويف الرجال او دو تواليت 100مل", "Missoni"),
    ("عطر جوسي كوتور ديرتي انجلش الرجال تواليت 100مل", "Juicy Couture"),
    ("عطر لانكم عود بوكيه أو دو برفان 100مل", "Lancome"),
    ("جيفنشي عطر جنتلمان سوسايتي أو دو برفان 100مل", "Givenchy"),
]
for name, brand in tests:
    pl = extract_product_line(name, brand)
    print(f"  {name[:50]:50s} → brand={brand:15s} → pline='{pl}'")

# === Test 3: Matching accuracy ===
print("\n" + "=" * 60)
print("  Test 3: Matching accuracy (key cases)")
print("=" * 60)

our_file = "منتجاتمهووستنسيقتحيثالاسعار.csv"
comp_file = "متجرساراستور.csv"

if our_file in dfs and comp_file in dfs:
    our_df = dfs[our_file]
    comp_df = dfs[comp_file]
    
    ccol = _fcol(comp_df, ["المنتج","اسم المنتج","Product","Name","name"])
    icol = _fcol(comp_df, ["ID","id","معرف","رقم المنتج","SKU","sku","الكود","code"])
    idx = CompIndex(comp_df, ccol, icol, "سارا ستور")
    
    # Test specific products
    test_products = [
        "عطر بربري هيرو أو دو تواليت 100مل (للرجال)",
        "تستر ميسوني ويف الرجال او دو تواليت 100مل",
        "عطر لانكم عود بوكيه أو دو برفان 100مل",
    ]
    
    for prod in test_products:
        brand = extract_brand(prod)
        size = extract_size(prod)
        ptype = extract_type(prod)
        gender = extract_gender(prod)
        norm = normalize(prod)
        pline = extract_product_line(prod, brand)
        
        results = idx.search(norm, brand, size, ptype, gender, our_pline=pline, top_n=3)
        
        print(f"\n🔍 منتجنا: {prod[:60]}")
        print(f"   brand={brand}, size={size}, pline='{pline}'")
        if results:
            for r in results:
                print(f"   → {r['name'][:60]:60s} | score={r['score']:5.1f}% | price={r['price']}")
        else:
            print(f"   → ❌ لا توجد مطابقة")
else:
    print(f"⚠️ Missing files: our={our_file in dfs}, comp={comp_file in dfs}")

print("\n✅ Done!")

