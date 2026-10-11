"""R25 参数快照生成与滚动（出价规格 §2.6 检验线参数表的可复算机制）。

只读消费 105 staged 外部成交明细（不写 105、不装新包、不联网）：
- 105 `03-估值引擎/data/staged/lianjia_ext/current.json` → 最新 run 的 lianjia_ext_sale_record.parquet
  （pyarrow 只读；解释器探测结果见 add-purchase-price-estimate change evidence）
- 06 `exports/manifest.csv`（清单小区集合，19 小区，随批次扩容）

口径（出价规格 §2.6 正本，本脚本为可复算实现，不手调数字）：
- 口径日＝staged run manifest（lianjia_ext_sale_record.manifest.json）built_at 的日期部分；
- 窗口＝[口径日 − 2 年, 口径日]（含端点），成交日期在窗口内才计入；
- R＝成交总价／最终挂牌价（全量 sale_record 口径，不筛普通住宅）；
  两字段缺失或挂牌价 ≤0 的行剔除并计数；
- 全市输出窗口内 R25（＝Q1）与四分位（Q1/Q2/Q3）与样本数 n；
- 各小区输出窗口内成交笔数与 R25：n ≥10 才出小区值，否则标「回落全市」；
- 小区匹配＝小区名精确匹配＋经用户逐个确认的含混映射（CONFIRMED_FUZZY，与 06
  deal_window.py 同值抄录）；新含混名不自行扩映射，记缺失并提示，不虚构。

输出本脚本上级目录的 `模型/exports/r25/`：
- `r25_table.csv`（当前生效）＋ `snapshots/r25_table_<口径日>.csv`（每次运行归档）
  ＋ `r25_meta.json`（run_id、口径日、窗口、生成时间戳、样本数、剔除行数、提示）；
- 运行前已存在 `r25_table.csv` 时，与本次结果逐行 diff 打印到 stdout；首跑打印
  「首跑无前值可比」。

与出价规格 §2.6 表的锚定校验出现非四舍五入级差异时，如实记录并中断上报，
不得手调任何数字（add-purchase-price-estimate 确认契约升级条件）。

用法:
    python r25_table.py [--manifest <csv>] [--out <dir>] [--src <staged dir>]
"""
import argparse
import collections
import csv
import datetime
import json
import os
import statistics

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - 环境不可用时的显式停报
    print("RESULT=STOP 缺少 parquet 读取依赖（pyarrow），按契约停止上报，不装新包")
    raise SystemExit(3)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
OUT_DIR = os.path.join(BASE_DIR, "exports", "r25")
# 数据依赖（仓库不含底层数据，运行时必须用 --manifest / --src 显式指向你的数据）：
# - manifest＝目标小区名单 csv（源系统中来自清单板块 06 的 exports/manifest.csv）；
# - src＝staged 成交明细 run 目录（源系统中来自估值引擎 105 的 lianjia_ext，含 current.json）。
MANIFEST_PATH = None
SRC_105_STAGED = None

# 含混匹配映射：小区编号 -> 明细中的小区名（仅收经使用者逐个确认的项，不自行扩映射；
# 开源版已移除真实映射，此处仅留结构说明）
CONFIRMED_FUZZY = {
    # 示例（占位）："数据源小区编号": "该小区在明细数据中的登记名",
}

THRESHOLD = 10          # 小区近两年成交 ≥10 笔才出小区值，否则回落全市
CITYWIDE_LABEL = "全市"
COMMUNITY_LABEL = "小区"
MATCH_EXACT = "精确"
MATCH_FUZZY = "含混（已确认）"
CSV_COLUMNS = ["层级", "小区名", "口径日", "窗口起", "窗口止",
               "n", "R25", "Q1", "Q2", "Q3", "备注"]
PRICE_FMT = "%.4f"      # R25 与四分位统一 4 位小数落盘（比较容差 ±0.0005）


def minus_two_years(day):
    """口径日减 2 年；2 月 29 日在平年回落到 2 月 28 日。"""
    try:
        return datetime.date(day.year - 2, day.month, day.day)
    except ValueError:
        return datetime.date(day.year - 2, day.month, 28)


def load_targets(path):
    """读 06 manifest，返回 小区编号 -> 小区名（保序去重）。"""
    with open(path, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    targets = collections.OrderedDict()
    for row in rows:
        cid = (row.get("小区编号") or "").strip()
        if cid and cid not in targets:
            targets[cid] = (row.get("小区名") or "").strip()
    return targets


def quartiles(values):
    """四分位（Q1/Q2/Q3，linear/inclusive 口径）；样本不足 2 返回 None。"""
    if len(values) < 2:
        return None
    return statistics.quantiles(values, n=4, method="inclusive")


def table_rows(cutoff, start, citywide, community_lines):
    """装配 r25_table.csv 的行（全市兜底行在前，小区行按清单顺序在后）。"""
    def price_cols(qs):
        if qs is None:
            return ["", "", "", ""]
        fmt = lambda x: PRICE_FMT % round(x, 4)
        return [fmt(qs[0]), fmt(qs[0]), fmt(qs[1]), fmt(qs[2])]  # R25＝Q1

    rows = [dict(zip(CSV_COLUMNS, [
        CITYWIDE_LABEL, "全市", cutoff.isoformat(), start.isoformat(), cutoff.isoformat(),
        citywide["n"]] + price_cols(citywide["quartiles"]) + ["兜底层级（小区 n<%d 时回落）" % THRESHOLD]))]
    for line in community_lines:
        notes = []
        if line["n"] < THRESHOLD:
            notes.append("回落全市（近两年成交<%d 笔）" % THRESHOLD)
        if line["match"] == MATCH_FUZZY:
            notes.append("含混匹配（已确认）")
        rows.append(dict(zip(CSV_COLUMNS, [
            COMMUNITY_LABEL, line["name"], cutoff.isoformat(), start.isoformat(), cutoff.isoformat(),
            line["n"]] + price_cols(line["quartiles"]) + ["；".join(notes)])))
    return rows


def write_table(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def print_diff(prev_path, new_rows):
    """与上一份当前快照逐行 diff（层级+小区名为主键，逐列比对）。"""
    with open(prev_path, encoding="utf-8-sig", newline="") as handle:
        prev_rows = list(csv.DictReader(handle))
    key = lambda row: (row["层级"], row["小区名"])
    prev = {key(r): r for r in prev_rows}
    new = {key(r): {c: str(r[c]) for c in CSV_COLUMNS} for r in new_rows}
    added = [k for k in new if k not in prev]
    removed = [k for k in prev if k not in new]
    changed = [k for k in new if k in prev and any(prev[k][c] != new[k][c] for c in CSV_COLUMNS)]
    print("")
    print("=== 与上一份当前快照 diff（%s）===" % prev_path)
    print("上份行数 = %d | 本次行数 = %d | 新增 %d / 移除 %d / 变更 %d"
          % (len(prev_rows), len(new_rows), len(added), len(removed), len(changed)))
    for k in added:
        r = new[k]
        print("  + %s/%s n=%s R25=%s" % (k[0], k[1], r["n"], r["R25"]))
    for k in removed:
        r = prev[k]
        print("  - %s/%s n=%s R25=%s" % (k[0], k[1], r["n"], r["R25"]))
    for k in changed:
        fields = ["%s: %s -> %s" % (c, prev[k][c], new[k][c])
                  for c in CSV_COLUMNS if prev[k][c] != new[k][c]]
        print("  ~ %s/%s %s" % (k[0], k[1], "；".join(fields)))
    if not (added or removed or changed):
        print("  （与上一份当前快照完全一致）")


def main():
    parser = argparse.ArgumentParser(description="R25 参数快照生成与滚动（出价规格 §2.6）")
    parser.add_argument("--manifest", required=True, help="目标小区名单 csv（无默认，仓库不含数据）")
    parser.add_argument("--out", default=OUT_DIR)
    parser.add_argument("--src", required=True, help="staged 成交明细 run 目录（无默认，仓库不含数据）")
    args = parser.parse_args()

    with open(os.path.join(args.src, "current.json"), encoding="utf-8") as handle:
        current = json.load(handle)
    rel = current["sale_record"].replace("/", os.sep)
    record = os.path.join(args.src, rel)
    if not os.path.exists(record):
        print("RESULT=STOP staged 明细不存在: %s" % record)
        return 3

    manifest_path = record[:-len(".parquet")] + ".manifest.json"
    built_at = None
    if os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as handle:
            built_at = json.load(handle).get("built_at")
    if not built_at:
        print("RESULT=STOP run manifest 缺 built_at，无法确定口径日")
        return 3
    cutoff = datetime.date.fromisoformat(built_at[:10])
    start = minus_two_years(cutoff)

    targets = load_targets(args.manifest)
    print("小区数 = %d" % len(targets))
    print("口径日（staged 快照）= %s" % cutoff.isoformat())
    print("窗口 = %s .. %s（含端点）" % (start.isoformat(), cutoff.isoformat()))
    print("数据源 = %s (run %s)" % (rel, current.get("run_id")))
    print("R 口径 = 成交总价／最终挂牌价（全量 sale_record，不筛普通住宅）")
    print("")

    try:
        table = pq.read_table(record, columns=["community_name", "sale_date",
                                               "total_price_yuan", "listing_price_yuan"])
    except Exception as exc:
        print("RESULT=STOP staged parquet 不可读（%s: %s），按契约停止上报，不静默降级"
              % (type(exc).__name__, exc))
        return 3
    names = table.column("community_name").to_pylist()
    days = table.column("sale_date").to_pylist()
    totals = table.column("total_price_yuan").to_pylist()
    listings = table.column("listing_price_yuan").to_pylist()

    wanted = {}
    for cid, name in targets.items():
        wanted[name] = cid
    for cid, name in CONFIRMED_FUZZY.items():
        wanted[name] = cid
    detail_of = {name: name for _, name in targets.items()}
    detail_of.update({name: name for cid, name in CONFIRMED_FUZZY.items()})

    lo, hi = start.isoformat(), cutoff.isoformat()
    citywide_rs = []
    buckets = collections.defaultdict(list)
    full_buckets = collections.defaultdict(int)
    n_in_window = n_out_window = n_undated = n_dropped = 0
    drop_missing_total = drop_missing_listing = drop_listing_le_zero = 0
    for name, day, total, listing in zip(names, days, totals, listings):
        if not day:
            n_undated += 1
            continue
        if not (lo <= day <= hi):
            n_out_window += 1
            continue
        n_in_window += 1
        miss_total = total is None
        miss_listing = listing is None
        if miss_total:
            drop_missing_total += 1
        if miss_listing:
            drop_missing_listing += 1
        if not miss_listing and listing <= 0:
            drop_listing_le_zero += 1
        if miss_total or miss_listing or (not miss_listing and listing <= 0):
            n_dropped += 1
            continue
        cid = wanted.get(name)
        ratio = float(total) / float(listing)
        citywide_rs.append(ratio)
        if cid:
            buckets[cid].append(ratio)
        if name in wanted:
            full_buckets[wanted[name]] += 1

    print("全量行 = %d | 窗口内 = %d | 窗口外 = %d | 无成交日期 = %d"
          % (len(names), n_in_window, n_out_window, n_undated))
    print("窗口内剔除行 = %d（其中缺成交总价 %d／缺最终挂牌价 %d〔与缺总价行有重叠〕／挂牌价≤0 %d）"
          "| 有效 R 样本 = %d"
          % (n_dropped, drop_missing_total, drop_missing_listing,
             drop_listing_le_zero, len(citywide_rs)))

    if not citywide_rs:
        print("RESULT=STOP 窗口内无有效 R 样本，无法生成全市 R25")
        return 3
    city_qs = quartiles(citywide_rs)

    print("")
    print("=== 全市（兜底） ===")
    print("  n = %d | R25 = %s | Q1/Q2/Q3 = %s / %s / %s"
          % (len(citywide_rs), PRICE_FMT % round(city_qs[0], 4),
             PRICE_FMT % round(city_qs[0], 4), PRICE_FMT % round(city_qs[1], 4),
             PRICE_FMT % round(city_qs[2], 4)))

    print("")
    print("=== 逐小区（阈值 %d 笔） ===" % THRESHOLD)
    community_lines = []
    for cid, name in targets.items():
        rows = buckets.get(cid, [])
        if cid in CONFIRMED_FUZZY:
            match = MATCH_FUZZY
        elif rows:
            match = MATCH_EXACT
        else:
            match = "无"
        qs = quartiles(rows)
        line = {
            "编号": cid,
            "name": name,
            "match": match,
            "n": len(rows),
            "quartiles": qs if len(rows) >= THRESHOLD else None,
            "full_n": full_buckets.get(cid, 0),
        }
        community_lines.append(line)
        if len(rows) >= THRESHOLD:
            value = "R25=%s" % (PRICE_FMT % round(qs[0], 4))
        else:
            value = "回落全市"
        print("  %-16s %-8s n=%-3d %s 全量匹配=%d 匹配=%s"
              % (cid, name, len(rows), value, line["full_n"], match))

    unmatched_hint = []
    for line in community_lines:
        if line["full_n"] == 0:
            unmatched_hint.append("%s（%s）：105 staged 明细无精确匹配且无经确认含混匹配，按未匹配处理"
                                  % (line["name"], line["编号"]))
    if unmatched_hint:
        print("")
        print("=== 未匹配小区提示（不自行扩映射、不虚构） ===")
        for hint in unmatched_hint:
            print("  ! %s" % hint)

    new_rows = table_rows(cutoff, start,
                          {"n": len(citywide_rs), "quartiles": city_qs},
                          community_lines)
    os.makedirs(args.out, exist_ok=True)
    os.makedirs(os.path.join(args.out, "snapshots"), exist_ok=True)
    current_path = os.path.join(args.out, "r25_table.csv")
    if os.path.exists(current_path):
        print_diff(current_path, new_rows)
    else:
        print("")
        print("首跑无前值可比")
    write_table(current_path, new_rows)
    write_table(os.path.join(args.out, "snapshots", "r25_table_%s.csv" % cutoff.isoformat()),
                new_rows)

    meta = {
        "run_id": current.get("run_id"),
        "staged_table": "lianjia_ext_sale_record",
        "caliber_date": cutoff.isoformat(),
        "window": {"start": start.isoformat(), "end": cutoff.isoformat()},
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "rows_total": len(names),
        "rows_in_window": n_in_window,
        "rows_outside_window": n_out_window,
        "rows_undated": n_undated,
        "excluded_in_window": {
            "rows": n_dropped,
            "missing_total_price": drop_missing_total,
            "missing_listing_price": drop_missing_listing,
            "listing_price_le_zero": drop_listing_le_zero,
        },
        "citywide": {
            "n": len(citywide_rs),
            "R25": round(city_qs[0], 4),
            "Q1": round(city_qs[0], 4),
            "Q2": round(city_qs[1], 4),
            "Q3": round(city_qs[2], 4),
        },
        "communities": [
            {
                "小区编号": line["编号"],
                "小区名": line["name"],
                "匹配方式": line["match"],
                "窗口内n": line["n"],
                "全量匹配n": line["full_n"],
                "R25": round(line["quartiles"][0], 4) if line["quartiles"] else None,
                "回落全市": line["n"] < THRESHOLD,
            }
            for line in community_lines
        ],
        "unmatched_hint": unmatched_hint,
    }
    with open(os.path.join(args.out, "r25_meta.json"), "w", encoding="utf-8") as handle:
        json.dump(meta, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    n_community_values = sum(1 for line in community_lines if line["n"] >= THRESHOLD)
    n_fallback = len(community_lines) - n_community_values
    print("")
    print("输出 -> %s（r25_table.csv / snapshots/r25_table_%s.csv / r25_meta.json）"
          % (args.out, cutoff.isoformat()))
    print("RESULT=OK 全市R25=%s 全市n=%d 窗口=%s..%s 小区值=%d 回落=%d"
          % (PRICE_FMT % round(city_qs[0], 4), len(citywide_rs),
             start.isoformat(), cutoff.isoformat(), n_community_values, n_fallback))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
