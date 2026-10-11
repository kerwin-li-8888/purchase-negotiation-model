"""WP4 回放 —— C10 A 档时点对齐价格参照检查（C22 操作化；一次性校验产物）。

只读消费 105 staged 外部成交明细（不写 105、不装新包、不联网）：
- 105 `03-估值引擎/data/staged/lianjia_ext/current.json` → run_20260831T041648Z 的
  lianjia_ext_sale_record.parquet（pyarrow 只读；current.json 指向与冻结 run 不一致即显式停报）
- 06 `exports/manifest.csv`（19 小区清单，冻结）
- `08-购房谈判策略模型/exports/r25/r25_table.csv`（任务 2.1 参数快照，基线二 R 值唯一来源；
  快照口径日／窗口与冻结不一致即显式停报）

冻结口径（概率规格 §5＝C10 A 档操作化；写死常量、运行时打印、不提供命令行改口）：
- 窗口＝2024-08-31 至 2026-08-31（含端点）；
- 代理＝成交日前 365 天同小区可比成交总价中位：同小区（精确匹配＋CONFIRMED_FUZZY 同值抄录）、
  可比成交日期严格早于该笔成交日、且在成交日前 365 天窗口内〔成交日−365 天 ≤ 可比成交日 < 成交日〕、
  可比成交总价非缺失；可比 ≥3 笔才计，不足记缺失并计缺失原因；代理非 M1（106 不越界）；
- 挂牌口径＝成交时点最终挂牌价（105 代码溯源，搭建方案 5.1 补记；非初始挂牌价）；
- T(δ)＝代理×(1+δ)，δ∈{0, −1%, −2%, −3%}；对实际成交总价算有符号偏差与绝对偏差（金额元＋比率双列）；
- 基线一＝不加修正代理（即 δ=0 档，另列单份基线列）；基线二＝最终挂牌×近期窗口 R 分位
  （R50 主／R25 辅，R 值取 r25 快照原值：全市行 Q2 与 R25；小区 n≥10 用小区 Q2/R25，
  否则回落全市，与检验线同规则；不重算快照值）；
- 分层＝按成交日所在年份（2024／2025／2026）。

回放成交笔集合＝匹配清单小区（精确＋CONFIRMED_FUZZY）＋窗口内＋成交总价与最终挂牌价均非缺失
＋挂牌价 >0（与 r25_table.py 剔除规则同款；审计基线 334 笔即同口径）；可比池＝匹配清单小区
＋成交总价非缺失的全历史成交（可比可早于窗口起点）。

本脚本属一次性校验产物：逐笔 CSV 与 summary 落输出目录（默认 `模型/exports/replay_wp4/`；源系统中随实施 change 归档），不落 R25 快照目录 `exports/r25/`；
可滚动的活产物只有 r25 参数快照（r25_table.py）。结论许可仅两条（概率规格 §5）：
①T 规则系统性偏差的方向性观察；②为 δ 档位校准提供线索——修改档位走 V1.5+ 流程，不自动改。
本报告与产物为事后参照检查：代理非 M1 估值、事后检查非策略业绩；不得据此宣称谈判成功率、
节省金额或规则有效（C6／C10）。

用法:
    python replay_price_reference.py [--manifest <csv>] [--r25 <csv>] [--out <dir>] [--src <dir>]
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
PROJECT_ROOT = os.path.dirname(BASE_DIR)
R25_SNAPSHOT_PATH = os.path.join(BASE_DIR, "exports", "r25", "r25_table.csv")
OUT_DIR = os.path.join(BASE_DIR, "exports", "replay_wp4")
# 数据依赖（仓库不含底层数据，运行时必须用 --manifest / --src 显式指向你的数据）：
# - manifest＝目标小区名单 csv（源系统中来自清单板块 06 的 exports/manifest.csv）；
# - src＝staged 成交明细 run 目录（源系统中来自估值引擎 105 的 lianjia_ext，含 current.json）。
MANIFEST_PATH = None
SRC_105_STAGED = None

# ===== 冻结输入（写死常量；与审计冻结口径一致，不提供命令行改口） =====
FROZEN_RUN_ID = "20260831T041648Z"
FROZEN_WINDOW_START = datetime.date(2024, 8, 31)
FROZEN_WINDOW_END = datetime.date(2026, 8, 31)
PROXY_LOOKBACK_DAYS = 365   # 可比窗口＝[成交日−365 天, 成交日)
PROXY_MIN_N = 3             # 可比 ≥3 笔才计，不足记缺失并计原因
DELTAS = [(0.0, "δ0"), (-0.01, "δ-1%"), (-0.02, "δ-2%"), (-0.03, "δ-3%")]
LISTING_CALIBER = "成交时点最终挂牌价（105 代码溯源口径；非初始挂牌价）"
PROXY_DEFINITION = ("成交日前 365 天同小区可比成交总价中位：同小区、可比成交日期严格早于该笔成交日、"
                    "且在〔成交日−365 天, 成交日〕窗口内、可比成交总价非缺失；可比 ≥3 笔才计，"
                    "不足记缺失并计缺失原因；代理非 M1（历史重算 M1 属 105 侧，106 只读不越界）")

# 含混匹配映射：小区编号 -> 明细中的小区名（仅收经使用者逐个确认的项，不自行扩映射；
# 开源版已移除真实映射，与 r25_table.py 同一规则）
CONFIRMED_FUZZY = {
    # 示例（占位）："数据源小区编号": "该小区在明细数据中的登记名",
}
THRESHOLD = 10              # 基线二层级：小区近两年成交 n≥10 用小区 Q2/R25，否则回落全市
AUDIT_TRADES = 334          # 审计基线：19 小区近两年窗口成交笔数（搭建方案 5.1 补记）
AUDIT_COVERAGE_GE3 = 0.934  # 审计基线：前 365 天同小区可比 ≥3 笔占比（搭建方案 5.1 补记）

CSV_BASE_COLUMNS = ["小区编号", "小区名", "成交日期", "成交总价_元", "最终挂牌价_元",
                    "代理_元", "代理可比笔数", "缺失原因"]
CSV_BASELINE2_COLUMNS = ["基线二_层级",
                         "基线二_R50值_元", "有符号偏差_R50_元", "有符号偏差_R50_比率",
                         "绝对偏差_R50_元", "绝对偏差_R50_比率",
                         "基线二_R25值_元", "有符号偏差_R25_元", "有符号偏差_R25_比率",
                         "绝对偏差_R25_元", "绝对偏差_R25_比率"]
AMT_FMT = "%.2f"    # 金额（元）
RAT_FMT = "%.6f"    # 比率


def load_targets(path):
    """读 06 manifest，返回 小区编号 -> 小区名（保序去重，与 r25_table.py 同款）。"""
    with open(path, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    targets = collections.OrderedDict()
    for row in rows:
        cid = (row.get("小区编号") or "").strip()
        if cid and cid not in targets:
            targets[cid] = (row.get("小区名") or "").strip()
    return targets


def load_r25_snapshot(path):
    """读 r25 快照（基线二 R 值唯一来源），做冻结一致性校验，不重算快照值。"""
    with open(path, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    citywide = None
    communities = {}
    for row in rows:
        if row["层级"] == "全市":
            citywide = row
        else:
            communities[(row["小区名"] or "").strip()] = row
    if citywide is None:
        print("RESULT=STOP r25 快照缺全市行: %s" % path)
        raise SystemExit(3)
    frozen = {"口径日": FROZEN_WINDOW_END.isoformat(),
              "窗口起": FROZEN_WINDOW_START.isoformat(),
              "窗口止": FROZEN_WINDOW_END.isoformat()}
    for key, expect in frozen.items():
        if (citywide.get(key) or "").strip() != expect:
            print("RESULT=STOP r25 快照 %s=%s 与冻结口径 %s 不一致（输入冻结破坏，停报）"
                  % (key, citywide.get(key), expect))
            raise SystemExit(3)
    city = {"n": int(citywide["n"]),
            "Q2": float(citywide["Q2"]),
            "R25": float(citywide["R25"])}
    for name, row in communities.items():
        n = int(row["n"])
        has_value = bool((row.get("Q2") or "").strip()) and bool((row.get("R25") or "").strip())
        if (n >= THRESHOLD) != has_value:
            print("RESULT=STOP r25 快照小区行层级不一致：%s n=%d 有值=%s（n≥%d 应有值，否则应回落）"
                  % (name, n, has_value, THRESHOLD))
            raise SystemExit(3)
    return city, communities


def stats_block(pairs):
    """pairs＝〔(有符号偏差_元, 有符号偏差_比率), …〕→ 有符号／绝对的中位与四分位（元＋比率）。"""
    out = {"n": len(pairs)}
    if not pairs:
        return out
    sy = [p[0] for p in pairs]
    sr = [p[1] for p in pairs]
    ay = [abs(p[0]) for p in pairs]
    ar = [abs(p[1]) for p in pairs]
    out["有符号"] = {"中位_元": round(statistics.median(sy), 2),
                    "中位_比率": round(statistics.median(sr), 6)}
    out["绝对"] = {"中位_元": round(statistics.median(ay), 2),
                   "中位_比率": round(statistics.median(ar), 6)}
    if len(pairs) >= 2:
        qy = statistics.quantiles(sy, n=4, method="inclusive")
        qr = statistics.quantiles(sr, n=4, method="inclusive")
        qay = statistics.quantiles(ay, n=4, method="inclusive")
        qar = statistics.quantiles(ar, n=4, method="inclusive")
        out["有符号"].update({"Q1_元": round(qy[0], 2), "Q3_元": round(qy[2], 2),
                              "Q1_比率": round(qr[0], 6), "Q3_比率": round(qr[2], 6)})
        out["绝对"].update({"Q1_元": round(qay[0], 2), "Q3_元": round(qay[2], 2),
                            "Q1_比率": round(qar[0], 6), "Q3_比率": round(qar[2], 6)})
    return out


def pairs_of(trades, key):
    """按偏差键取有符号 (元, 比率) 对；键缺失（代理缺失笔）跳过。"""
    return [(t[key][1], t[key][2]) for t in trades if t.get(key) is not None]


def main():
    parser = argparse.ArgumentParser(description="WP4 回放：C10 A 档时点对齐价格参照检查（C22）")
    parser.add_argument("--manifest", required=True, help="目标小区名单 csv（无默认，仓库不含数据）")
    parser.add_argument("--r25", default=R25_SNAPSHOT_PATH)
    parser.add_argument("--out", default=OUT_DIR)
    parser.add_argument("--src", required=True, help="staged 成交明细 run 目录（无默认，仓库不含数据）")
    args = parser.parse_args()

    # ===== 冻结输入打印 =====
    targets = load_targets(args.manifest)
    print("=== 冻结输入（C10 A 档操作化，写死常量，运行时打印） ===")
    print("小区清单 = %d 个（06 exports/manifest.csv，保序）" % len(targets))
    for cid, name in targets.items():
        print("  %s %s" % (cid, name))
    print("窗口 = %s .. %s（含端点）" % (FROZEN_WINDOW_START.isoformat(), FROZEN_WINDOW_END.isoformat()))
    print("staged run = run_%s（current.json 指向不一致即显式停报）" % FROZEN_RUN_ID)
    print("代理 = %s" % PROXY_DEFINITION)
    print("挂牌口径 = %s" % LISTING_CALIBER)
    print("δ 档 = %s（T＝代理×(1+δ)，偏差对实际成交总价：有符号 T−成交价 与绝对值，金额元＋比率）"
          % "、".join(label for _, label in DELTAS))
    print("基线一 = 不加修正代理（δ=0 档另列基线列）；基线二 = 最终挂牌×近期窗口 R 分位"
          "（R50 主／R25 辅，取 r25 快照原值：全市行 Q2 与 R25；小区 n≥%d 用小区 Q2/R25，否则回落全市）"
          % THRESHOLD)
    print("匹配契约 = 小区名精确匹配＋CONFIRMED_FUZZY 同值抄录（%s），不自行扩映射"
          % "；".join("%s→%s" % (cid, name) for cid, name in CONFIRMED_FUZZY.items()))
    print("分层 = 按成交日所在年份（%s）" % "／".join(
        str(y) for y in range(FROZEN_WINDOW_START.year, FROZEN_WINDOW_END.year + 1)))
    print("")

    # ===== 基线二 R 值来源（r25 快照，冻结校验） =====
    city, snap_communities = load_r25_snapshot(args.r25)
    missing_snap = [name for name in targets.values() if name not in snap_communities]
    if missing_snap:
        print("RESULT=STOP 清单小区不在 r25 快照中（快照与清单不一致，停报）：%s"
              % "；".join(missing_snap))
        return 3
    level_source = {}   # cid -> (层级, Q2, R25, n)
    for cid, name in targets.items():
        row = snap_communities[name]
        n = int(row["n"])
        if n >= THRESHOLD:
            level_source[cid] = ("小区", float(row["Q2"]), float(row["R25"]), n)
        else:
            level_source[cid] = ("全市", city["Q2"], city["R25"], n)
    print("=== 基线二 R 值来源（r25 快照，取原值不重算） ===")
    print("快照 = %s" % args.r25)
    print("快照口径日 = %s | 窗口 = %s..%s | 全市 n = %d | 全市 Q2(R50) = %s | 全市 R25 = %s"
          % (FROZEN_WINDOW_END.isoformat(), FROZEN_WINDOW_START.isoformat(),
             FROZEN_WINDOW_END.isoformat(), city["n"],
             ("%.4f" % city["Q2"]), ("%.4f" % city["R25"])))
    print("用小区值小区数 = %d（n≥%d），回落全市 = %d"
          % (sum(1 for v in level_source.values() if v[0] == "小区"), THRESHOLD,
             sum(1 for v in level_source.values() if v[0] == "全市")))
    for cid, name in targets.items():
        lvl, q2, r25, n = level_source[cid]
        print("  %s %-12s n=%-3d 层级=%s Q2=%s R25=%s"
              % (cid, name, n, lvl,
                 ("%.4f" % q2) if lvl == "小区" else ("%.4f(全市)" % q2),
                 ("%.4f" % r25) if lvl == "小区" else ("%.4f(全市)" % r25)))
    print("")

    # ===== 读 staged（冻结 run） =====
    with open(os.path.join(args.src, "current.json"), encoding="utf-8") as handle:
        current = json.load(handle)
    if current.get("run_id") != FROZEN_RUN_ID:
        print("RESULT=STOP staged current.json 指向 run %s，与冻结 run_%s 不一致"
              "（输入冻结破坏，停报，不静默换源）" % (current.get("run_id"), FROZEN_RUN_ID))
        return 3
    rel = current["sale_record"].replace("/", os.sep)
    record = os.path.join(args.src, rel)
    if not os.path.exists(record):
        print("RESULT=STOP staged 明细不存在: %s" % record)
        return 3
    print("数据源 = %s (run %s)（只读）" % (rel, current.get("run_id")))

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

    # 可比池：匹配清单小区、成交总价非缺失的全历史成交（可比可早于回放窗口起点）
    pool = collections.defaultdict(list)   # cid -> [(成交日, 成交总价)]
    replay = []                            # 逐笔回放对象
    n_unparsed_date = 0
    n_matched_rows = 0
    for name, day, total, listing in zip(names, days, totals, listings):
        cid = wanted.get(name)
        if cid is None:
            continue
        n_matched_rows += 1
        try:
            d = datetime.date.fromisoformat(day)
        except (TypeError, ValueError):
            n_unparsed_date += 1
            continue
        if total is not None:
            pool[cid].append((d, float(total)))
        if not (FROZEN_WINDOW_START <= d <= FROZEN_WINDOW_END):
            continue
        if total is None or listing is None or listing <= 0:
            continue  # 与 r25_table.py 同款剔除（缺总价／缺挂牌／挂牌 ≤0）
        replay.append({"cid": cid, "name": name, "date": d,
                       "total": float(total), "listing": float(listing)})

    # ===== 逐笔计算：代理、四档 T 与偏差、两基线 =====
    covered = 0
    for t in replay:
        lo = t["date"] - datetime.timedelta(days=PROXY_LOOKBACK_DAYS)
        comps = [v for (cd, v) in pool[t["cid"]] if lo <= cd < t["date"]]
        t["comp_n"] = len(comps)
        if len(comps) >= PROXY_MIN_N:
            t["proxy"] = statistics.median(comps)
            t["missing_reason"] = ""
            covered += 1
        else:
            t["proxy"] = None
            t["missing_reason"] = ("前365天同小区可比=0笔" if len(comps) == 0
                                   else "前365天同小区可比不足3笔（%d笔）" % len(comps))
        lvl, q2, r25, _ = level_source[t["cid"]]
        t["level"] = lvl
        t["dev_r50"] = None
        t["dev_r25"] = None
        if t["proxy"] is not None:
            for delta, label in DELTAS:
                tv = t["proxy"] * (1.0 + delta)
                signed = tv - t["total"]
                t["dev_%s" % label] = (tv, signed, signed / t["total"],
                                       abs(signed), abs(signed) / t["total"])
            t["dev_base1"] = t["dev_δ0"]  # 基线一＝不加修正代理（δ=0）
        for key, rv in (("dev_r50", q2), ("dev_r25", r25)):
            bv = t["listing"] * rv
            signed = bv - t["total"]
            t[key] = (bv, signed, signed / t["total"], abs(signed), abs(signed) / t["total"])

    # ===== 排序与逐笔 CSV =====
    replay.sort(key=lambda t: (t["date"], t["cid"]))
    delta_columns = []
    for _, label in DELTAS:
        delta_columns += ["T_%s_元" % label,
                          "有符号偏差_%s_元" % label, "有符号偏差_%s_比率" % label,
                          "绝对偏差_%s_元" % label, "绝对偏差_%s_比率" % label]
    baseline1_columns = ["基线一_无修正代理_元",
                         "有符号偏差_基线一_元", "有符号偏差_基线一_比率",
                         "绝对偏差_基线一_元", "绝对偏差_基线一_比率"]
    columns = CSV_BASE_COLUMNS + delta_columns + baseline1_columns + CSV_BASELINE2_COLUMNS

    def fmt_or_blank(value, fmt):
        return (fmt % value) if value is not None else ""

    csv_path = os.path.join(args.out, "replay_trades.csv")
    os.makedirs(args.out, exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for t in replay:
            row = [t["cid"], t["name"], t["date"].isoformat(),
                   AMT_FMT % t["total"], AMT_FMT % t["listing"],
                   fmt_or_blank(t["proxy"], AMT_FMT), t["comp_n"], t["missing_reason"]]
            for _, label in DELTAS:
                dev = t.get("dev_%s" % label)
                if dev is None:
                    row += ["", "", "", "", ""]
                else:
                    row += [fmt_or_blank(dev[0], AMT_FMT), fmt_or_blank(dev[1], AMT_FMT),
                            fmt_or_blank(dev[2], RAT_FMT), fmt_or_blank(dev[3], AMT_FMT),
                            fmt_or_blank(dev[4], RAT_FMT)]
            b1 = t.get("dev_base1")
            if b1 is None:
                row += ["", "", "", "", ""]
            else:
                row += [fmt_or_blank(b1[0], AMT_FMT), fmt_or_blank(b1[1], AMT_FMT),
                        fmt_or_blank(b1[2], RAT_FMT), fmt_or_blank(b1[3], AMT_FMT),
                        fmt_or_blank(b1[4], RAT_FMT)]
            lvl, _, _, _ = level_source[t["cid"]]
            r50 = t["dev_r50"]
            r25v = t["dev_r25"]
            row += [lvl,
                    fmt_or_blank(r50[0], AMT_FMT), fmt_or_blank(r50[1], AMT_FMT),
                    fmt_or_blank(r50[2], RAT_FMT), fmt_or_blank(r50[3], AMT_FMT),
                    fmt_or_blank(r50[4], RAT_FMT),
                    fmt_or_blank(r25v[0], AMT_FMT), fmt_or_blank(r25v[1], AMT_FMT),
                    fmt_or_blank(r25v[2], RAT_FMT), fmt_or_blank(r25v[3], AMT_FMT),
                    fmt_or_blank(r25v[4], RAT_FMT)]
            writer.writerow(row)

    # ===== 聚合统计 =====
    total_n = len(replay)
    cov_rate = (covered / total_n) if total_n else 0.0
    missing_counter = collections.Counter(t["missing_reason"] for t in replay
                                          if t["missing_reason"])
    level_counter = collections.Counter(t["level"] for t in replay)

    delta_stats = collections.OrderedDict()
    for _, label in DELTAS:
        delta_stats[label] = stats_block(pairs_of(replay, "dev_%s" % label))
    baseline_stats = collections.OrderedDict()
    baseline_stats["基线一_无修正代理（δ=0，逐笔值同 δ0 列）"] = stats_block(pairs_of(replay, "dev_base1"))
    baseline_stats["基线二_挂牌×R50（主）"] = {
        "层级分布": dict(level_counter), **stats_block(pairs_of(replay, "dev_r50"))}
    baseline_stats["基线二_挂牌×R25（辅）"] = {
        "层级分布": dict(level_counter), **stats_block(pairs_of(replay, "dev_r25"))}

    def r_values(trades):
        return [t["total"] / t["listing"] for t in trades]

    def r_block(trades):
        rs = r_values(trades)
        out = {"n": len(rs)}
        if not rs:
            return out
        out["中位"] = round(statistics.median(rs), 4)
        if len(rs) >= 2:
            qs = statistics.quantiles(rs, n=4, method="inclusive")
            out["Q1"] = round(qs[0], 4)
            out["Q3"] = round(qs[2], 4)
        return out

    actual_r = r_block(replay)

    by_year = collections.OrderedDict()
    for year in sorted({t["date"].year for t in replay}):
        subset = [t for t in replay if t["date"].year == year]
        sub_covered = sum(1 for t in subset if t["proxy"] is not None)
        entry = collections.OrderedDict()
        entry["笔数"] = len(subset)
        entry["覆盖笔数"] = sub_covered
        entry["覆盖率"] = round(sub_covered / len(subset), 4) if subset else 0.0
        entry["实际R（成交总价／最终挂牌价）"] = r_block(subset)
        for _, label in DELTAS:
            entry["δ档_%s" % label] = stats_block(pairs_of(subset, "dev_%s" % label))
        entry["基线二_挂牌×R50"] = stats_block(pairs_of(subset, "dev_r50"))
        entry["基线二_挂牌×R25"] = stats_block(pairs_of(subset, "dev_r25"))
        by_year[str(year)] = entry

    summary = collections.OrderedDict()
    summary["定位"] = ("事后参照检查：市场参照代理非 M1 估值、事后检查非策略业绩；"
                       "不得据此宣称谈判成功率、节省金额或规则有效（C6／C10）。"
                       "结论许可仅两条：①T 规则系统性偏差的方向性观察；②为 δ 档位校准提供线索"
                       "（修改档位走 V1.5+ 流程，不自动改）。一次性校验产物，不落 08/exports。")
    summary["冻结输入"] = {
        "run_id": FROZEN_RUN_ID,
        "窗口": {"start": FROZEN_WINDOW_START.isoformat(), "end": FROZEN_WINDOW_END.isoformat()},
        "代理定义": PROXY_DEFINITION,
        "挂牌口径": LISTING_CALIBER,
        "小区清单": [{"小区编号": cid, "小区名": name} for cid, name in targets.items()],
        "匹配契约": "精确匹配＋CONFIRMED_FUZZY 同值抄录（与 06 deal_window.py、r25_table.py 同值），不自行扩映射",
        "δ档": [label for _, label in DELTAS],
    }
    summary["r25快照（基线二R值来源）"] = {
        "路径": args.r25,
        "口径日": FROZEN_WINDOW_END.isoformat(),
        "全市": {"n": city["n"], "Q2(R50)": round(city["Q2"], 4), "R25": round(city["R25"], 4)},
        "层级分布（逐笔所在层级）": dict(level_counter),
    }
    summary["覆盖与缺失"] = {
        "总笔数": total_n,
        "覆盖笔数": covered,
        "覆盖率": round(cov_rate, 4),
        "缺失笔数": total_n - covered,
        "缺失原因分布": dict(missing_counter),
        "审计基线对照": {
            "审计窗口笔数": AUDIT_TRADES,
            "审计≥3笔覆盖率": AUDIT_COVERAGE_GE3,
            "回放窗口笔数": total_n,
            "回放覆盖率": round(cov_rate, 4),
            "笔数偏差": round((total_n - AUDIT_TRADES) / AUDIT_TRADES, 4) if AUDIT_TRADES else None,
            "覆盖率偏差（百分点）": round((cov_rate - AUDIT_COVERAGE_GE3) * 100, 2),
        },
    }
    summary["实际R（成交总价／最终挂牌价，全窗口）"] = actual_r
    summary["四档δ偏差"] = delta_stats
    summary["两基线"] = baseline_stats
    summary["按成交年分层"] = by_year
    summary["生成时间"] = datetime.datetime.now().isoformat(timespec="seconds")

    summary_path = os.path.join(args.out, "replay_summary.json")
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    # ===== stdout 汇总 =====
    print("")
    print("匹配清单小区行（全历史） = %d | 无法解析成交日 = %d" % (n_matched_rows, n_unparsed_date))
    print("回放逐笔 = %d 行 | 代理覆盖 = %d（%.1f%%） | 缺失 = %d"
          % (total_n, covered, 100.0 * cov_rate, total_n - covered))
    print("缺失原因分布：%s" % ("；".join("%s → %d 笔" % (k, v)
                                         for k, v in sorted(missing_counter.items())) or "（无）"))
    print("基线二层级分布（逐笔）：%s" % "；".join("%s %d 笔" % (k, v)
                                                 for k, v in sorted(level_counter.items())))
    print("实际R（成交总价／最终挂牌价）中位 = %.4f | Q1/Q3 = %s / %s"
          % (actual_r.get("中位", float("nan")),
             ("%.4f" % actual_r["Q1"]) if "Q1" in actual_r else "-",
             ("%.4f" % actual_r["Q3"]) if "Q3" in actual_r else "-"))
    print("")
    print("=== 四档 δ 偏差（代理可得笔，有符号 T−成交价；金额万元＝元/1e4） ===")
    for _, label in DELTAS:
        s = delta_stats[label]
        if s["n"]:
            print("  %-6s n=%-3d 有符号中位 = %12.2f 元（%+.4f）| Q1/Q3 = %s / %s 元（%s / %s）"
                  " | 绝对中位 = %10.2f 元（%.4f）"
                  % (label, s["n"], s["有符号"]["中位_元"], s["有符号"]["中位_比率"],
                     ("%.2f" % s["有符号"]["Q1_元"]), ("%.2f" % s["有符号"]["Q3_元"]),
                     ("%.4f" % s["有符号"]["Q1_比率"]), ("%.4f" % s["有符号"]["Q3_比率"]),
                     s["绝对"]["中位_元"], s["绝对"]["中位_比率"]))
        else:
            print("  %-6s n=0（无样本）" % label)
    print("")
    print("=== 两基线（同表可比） ===")
    for name, s in baseline_stats.items():
        if s.get("n"):
            print("  %s n=%-3d 有符号中位 = %12.2f 元（%+.4f）| 绝对中位 = %10.2f 元（%.4f）"
                  % (name, s["n"], s["有符号"]["中位_元"], s["有符号"]["中位_比率"],
                     s["绝对"]["中位_元"], s["绝对"]["中位_比率"]))
        else:
            print("  %s n=0（无样本）" % name)
    print("")
    print("=== 按成交年分层 ===")
    for year, entry in by_year.items():
        print("  %s 笔数=%-3d 覆盖=%-3d（%.1f%%）实际R中位=%s | δ0 有符号中位 %+.4f | R50 基线有符号中位 %+.4f | R25 基线有符号中位 %+.4f"
              % (year, entry["笔数"], entry["覆盖笔数"], 100.0 * entry["覆盖率"],
                 ("%.4f" % entry["实际R（成交总价／最终挂牌价）"]["中位"])
                 if entry["实际R（成交总价／最终挂牌价）"].get("中位") is not None else "-",
                 entry["δ档_δ0"]["有符号"]["中位_比率"] if entry["δ档_δ0"]["n"] else float("nan"),
                 entry["基线二_挂牌×R50"]["有符号"]["中位_比率"] if entry["基线二_挂牌×R50"]["n"] else float("nan"),
                 entry["基线二_挂牌×R25"]["有符号"]["中位_比率"] if entry["基线二_挂牌×R25"]["n"] else float("nan")))
    print("")

    # ===== 审计基线对照（不手调，显著背离如实上报） =====
    trade_dev = abs(total_n - AUDIT_TRADES) / AUDIT_TRADES if AUDIT_TRADES else 0.0
    print("审计对照：逐笔 %d 行 vs 审计 %d 笔（偏差 %+.1f%%）"
          % (total_n, AUDIT_TRADES, 100.0 * (total_n - AUDIT_TRADES) / AUDIT_TRADES))
    if trade_dev > 0.05:
        print("!! 逐笔行数与审计基线显著背离（±>5%%），不手调、不改冻结口径，"
              "触发确认契约升级条件，如实上报")
    print("审计对照：代理覆盖 %.1f%% vs 审计 ≥3 笔占比 %.1f%%（偏差 %+.1f 个百分点）"
          % (100.0 * cov_rate, 100.0 * AUDIT_COVERAGE_GE3,
             100.0 * (cov_rate - AUDIT_COVERAGE_GE3)))
    if cov_rate < AUDIT_COVERAGE_GE3 - 0.05:
        print("!! 回放覆盖率显著低于审计结论（>5 个百分点），不手调、不改冻结口径，"
              "触发确认契约升级条件，如实上报")
    print("")
    print("输出 -> %s（replay_trades.csv / replay_summary.json）" % args.out)
    print("RESULT=OK 逐笔%d行 覆盖%d（%.1f%%）" % (total_n, covered, 100.0 * cov_rate))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
