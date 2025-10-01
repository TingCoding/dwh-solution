import os
import json
import glob
import zipfile
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
import warnings

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)

DEFAULT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = os.environ.get("DATA_ROOT", str(DEFAULT_ROOT))

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 200)


def ensure_data_unzipped() -> str:
    root = DATA_ROOT
    if root.lower().endswith(".zip"):
        out_dir = root[:-4]
        if not os.path.exists(out_dir):
            with zipfile.ZipFile(root, 'r') as z:
                z.extractall(out_dir)
        return out_dir
    return root


# ---------- IO Helpers ----------

def _iter_event_files(table_dir: str) -> List[str]:
    return sorted(
        glob.glob(os.path.join(table_dir, "**", "*.json"), recursive=True) +
        glob.glob(os.path.join(table_dir, "**", "*.jsonl"), recursive=True)
    )


def read_event_logs(table_dir: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for path in _iter_event_files(table_dir):
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                continue

            def add_row(obj: Dict[str, Any]):
                o = dict(obj)
                o["_source_file"] = os.path.relpath(path, table_dir)
                rows.append(o)

            try:
                data = json.loads(content)
                if isinstance(data, list):
                    for evt in data:
                        add_row(evt)
                else:
                    add_row(data)
                continue
            except Exception:
                pass

            for i, line in enumerate(content.splitlines(), start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                    add_row(evt)
                except Exception:
                    print("\n❌ JSON ERROR in file:", path)
                    print(f"Bad line #{i}:", line[:200])
                    raise

    if not rows:
        return pd.DataFrame(columns=["id", "op", "ts", "data", "set", "_source_file"])

    df = pd.DataFrame(rows)
    df["_ts"] = pd.to_datetime(df.get("ts"), errors="coerce")
    op_order = {"c": 0, "u": 1}
    df["_op_order"] = df["op"].map(op_order).fillna(9)
    return df.sort_values(by=["_ts", "_op_order"]).reset_index(drop=True)


# ---------- Build versioned history per table ----------

def build_history(df_events: pd.DataFrame, id_col: str = "id") -> pd.DataFrame:
    """
    Build a versioned history snapshot: one row per event = state AFTER the event.
    """
    if df_events.empty:
        return pd.DataFrame()

    fields = set()
    for col in ("data", "set"):
        if col in df_events.columns:
            for v in df_events[col].dropna():
                if isinstance(v, dict):
                    fields.update(v.keys())
    fields = sorted(fields)

    state_by_id: Dict[Any, Dict[str, Any]] = {}
    ver_by_id: Dict[Any, int] = {}
    out: List[Dict[str, Any]] = []

    for _, evt in df_events.iterrows():
        rid = evt.get(id_col)
        ts = evt.get("ts")
        op = evt.get("op")

        if rid not in state_by_id:
            state_by_id[rid] = {}
            ver_by_id[rid] = 0

        if op == "c":
            payload = evt.get("data") or {}
            for k in fields:
                state_by_id[rid][k] = payload.get(k, state_by_id[rid].get(k, None))
        elif op == "u":
            payload = evt.get("set") or {}
            for k, v in payload.items():
                state_by_id[rid][k] = v
            for k in fields:
                state_by_id[rid].setdefault(k, None)
        else:
            for k in fields:
                state_by_id[rid].setdefault(k, None)

        ver_by_id[rid] += 1
        row = {"record_id": rid, "version": ver_by_id[rid], "ts": ts}
        for k in fields:
            row[k] = state_by_id[rid].get(k, None)
        out.append(row)

    hist = pd.DataFrame(out)
    hist["_ts"] = pd.to_datetime(hist["ts"], errors="coerce")
    return hist.sort_values(by=["record_id", "_ts", "version"]).drop(columns=["_ts"]).reset_index(drop=True)


# ---------- Table discovery (handle saving) ----------

def find_table_dirs(data_dir: str) -> Dict[str, Optional[str]]:
    candidates = {
        "accounts": ["accounts"],
        "cards": ["cards"],
        "saving_accounts": ["saving_accounts", "savings_accounts", "saving", "savings"],
    }
    found: Dict[str, Optional[str]] = {}
    for canon, opts in candidates.items():
        hit = next((n for n in opts if os.path.isdir(os.path.join(data_dir, n))), None)
        found[canon] = os.path.join(data_dir, hit) if hit else None
    return found


# ---------- Build denormalized joined timeline ----------

def build_denorm_join(histories: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Build denormalized view by joining accounts, cards, and saving_accounts on account_id.
    Uses forward-fill to propagate state changes across time.
    FIXED: Uses ffill() instead of deprecated fillna(method='ffill')
    """
    acc = histories.get("accounts", pd.DataFrame()).copy()
    card = histories.get("cards", pd.DataFrame()).copy()
    sav = histories.get("saving_accounts", pd.DataFrame()).copy()

    print("\nAvailable columns:")
    print(f"  accounts: {list(acc.columns) if not acc.empty else 'EMPTY'}")
    print(f"  cards: {list(card.columns) if not card.empty else 'EMPTY'}")
    print(f"  saving_accounts: {list(sav.columns) if not sav.empty else 'EMPTY'}")

    if acc.empty or (card.empty and sav.empty):
        print("Missing required data for join")
        return pd.DataFrame(), None

    def find_account_id_col(df: pd.DataFrame, table_name: str) -> Optional[str]:
        if df.empty:
            return None
        candidates = ['account_id', 'accountId', 'acc_id', 'record_id']
        for col in candidates:
            if col in df.columns:
                print(f"{table_name}: using '{col}' as account identifier")
                return col
        if table_name == 'accounts' and 'record_id' in df.columns:
            print(f"{table_name}: using 'record_id' as account_id")
            return 'record_id'
        return None

    acc_id_col = find_account_id_col(acc, 'accounts')
    card_id_col = find_account_id_col(card, 'cards')
    sav_id_col = find_account_id_col(sav, 'saving_accounts')

    if not acc_id_col:
        print("Cannot find account_id column in accounts table")
        return pd.DataFrame(), None

    acc_prep = acc.copy()
    if acc_id_col != 'account_id':
        acc_prep['account_id'] = acc_prep[acc_id_col]
    
    for col in acc_prep.columns:
        if col not in ['ts', 'account_id']:
            acc_prep.rename(columns={col: f'account_{col}'}, inplace=True)

    card_prep = pd.DataFrame()
    if not card.empty and card_id_col:
        card_prep = card.copy()
        if card_id_col != 'account_id':
            card_prep['account_id'] = card_prep[card_id_col]
        for col in card_prep.columns:
            if col not in ['ts', 'account_id']:
                card_prep.rename(columns={col: f'card_{col}'}, inplace=True)

    sav_prep = pd.DataFrame()
    if not sav.empty and sav_id_col:
        sav_prep = sav.copy()
        if sav_id_col != 'account_id':
            sav_prep['account_id'] = sav_prep[sav_id_col]
        for col in sav_prep.columns:
            if col not in ['ts', 'account_id']:
                sav_prep.rename(columns={col: f'saving_{col}'}, inplace=True)

    all_events = []
    
    if not acc_prep.empty:
        acc_prep['_source'] = 'account'
        all_events.append(acc_prep)
    
    if not card_prep.empty:
        card_prep['_source'] = 'card'
        all_events.append(card_prep)
    
    if not sav_prep.empty:
        sav_prep['_source'] = 'saving'
        all_events.append(sav_prep)

    if not all_events:
        print("No events to combine")
        return pd.DataFrame(), None

    combined = pd.concat(all_events, ignore_index=True, sort=False)
    combined['_ts'] = pd.to_datetime(combined['ts'], errors='coerce')
    combined = combined.sort_values(by=['account_id', '_ts', '_source']).reset_index(drop=True)

    print(f"Combined events: {len(combined)} rows")

    combined = combined.groupby('account_id', group_keys=False).apply(
        lambda g: g.ffill()
    ).reset_index(drop=True)

    combined = combined.drop(columns=['_source', '_ts'])

    rename_map = {}
    for col in combined.columns:
        if 'balance' in col.lower() and 'saving' in col:
            rename_map[col] = 'saving_balance'
        elif 'credit_used' in col.lower() and 'card' in col:
            rename_map[col] = 'card_credit_used'
    
    if rename_map:
        combined = combined.rename(columns=rename_map)

    print(f"Final joined table: {len(combined)} rows, {len(combined.columns)} columns")
    print(f"Final columns: {list(combined.columns)}")

    return combined, card_id_col or sav_id_col


# ---------- Detect transactions ----------

def detect_transactions(joined: pd.DataFrame) -> pd.DataFrame:
    """
    Detect transactions by looking for changes in saving_balance or card_credit_used.
    """
    if joined.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []

    print(f"\nDetecting transactions from {len(joined)} rows")
    print(f"Available columns: {list(joined.columns)}")

    if 'saving_balance' in joined.columns and 'account_id' in joined.columns:
        print("Checking saving_balance changes...")
        sav_cols = ['account_id', 'ts', 'saving_balance']
        
        rec_col = None
        for col in ['saving_record_id', 'record_id']:
            if col in joined.columns:
                rec_col = col
                sav_cols.append(col)
                break
        
        sav_data = joined[sav_cols].dropna(subset=['saving_balance']).copy()
        sav_data['saving_balance'] = pd.to_numeric(sav_data['saving_balance'], errors='coerce')
        sav_data = sav_data.dropna(subset=['saving_balance'])
        
        print(f"Found {len(sav_data)} saving balance records")
        
        group_cols = ['account_id']
        if rec_col:
            group_cols.append(rec_col)
        
        for group_key, g in sav_data.groupby(group_cols):
            g = g.sort_values(by='ts')
            prev_balance = None
            
            for _, row in g.iterrows():
                cur_balance = row['saving_balance']
                
                if prev_balance is not None and cur_balance != prev_balance:
                    delta = cur_balance - prev_balance
                    rows.append({
                        'type': 'saving_balance_change',
                        'record_group': f"saving:{group_key if isinstance(group_key, str) else '_'.join(map(str, group_key))}",
                        'ts': row['ts'],
                        'account_id': row['account_id'],
                        'delta': delta,
                        'value_after': cur_balance
                    })
                
                prev_balance = cur_balance

    if 'card_credit_used' in joined.columns and 'account_id' in joined.columns:
        print("Checking card_credit_used changes...")
        card_cols = ['account_id', 'ts', 'card_credit_used']
        
        rec_col = None
        for col in ['card_record_id', 'record_id']:
            if col in joined.columns:
                rec_col = col
                card_cols.append(col)
                break
        
        card_data = joined[card_cols].dropna(subset=['card_credit_used']).copy()
        card_data['card_credit_used'] = pd.to_numeric(card_data['card_credit_used'], errors='coerce')
        card_data = card_data.dropna(subset=['card_credit_used'])
        
        print(f"Found {len(card_data)} card credit_used records")
        
        group_cols = ['account_id']
        if rec_col:
            group_cols.append(rec_col)
        
        for group_key, g in card_data.groupby(group_cols):
            g = g.sort_values(by='ts')
            prev_credit = None
            
            for _, row in g.iterrows():
                cur_credit = row['card_credit_used']
                
                if prev_credit is not None and cur_credit != prev_credit:
                    delta = cur_credit - prev_credit
                    rows.append({
                        'type': 'card_credit_used_change',
                        'record_group': f"card:{group_key if isinstance(group_key, str) else '_'.join(map(str, group_key))}",
                        'ts': row['ts'],
                        'account_id': row['account_id'],
                        'delta': delta,
                        'value_after': cur_credit
                    })
                
                prev_credit = cur_credit

    print(f"Detected {len(rows)} transactions")

    if not rows:
        return pd.DataFrame()

    tx = pd.DataFrame(rows)
    tx['_ts'] = pd.to_datetime(tx['ts'], errors='coerce')
    tx = tx.sort_values(by='_ts').drop(columns=['_ts']).reset_index(drop=True)
    
    return tx


# ---------- Display helpers for clean output ----------

def format_dataframe_for_display(df: pd.DataFrame, max_rows: int = None) -> str:
    """
    Format DataFrame for display with cleaner NaN handling.
    Replaces NaN/None with empty string for better readability.
    """
    if df.empty:
        return "(empty)"
    
    display_df = df.copy()
    
    display_df = display_df.fillna('')
    
    if max_rows:
        return display_df.head(max_rows).to_string(index=False)
    return display_df.to_string(index=False)


# ---------- Main ----------

def main():
    data_root = ensure_data_unzipped()
    data_dir = os.path.join(data_root, "data")
    if not os.path.isdir(data_dir):
        raise SystemExit(f"Data directory not found at: {data_dir}")

    table_dirs = find_table_dirs(data_dir)
    histories: Dict[str, pd.DataFrame] = {}
    for name, tdir in table_dirs.items():
        print(f"\n===== Historical view: {name} =====")
        if not tdir:
            print("(missing directory)")
            histories[name] = pd.DataFrame()
            continue
        events = read_event_logs(tdir)
        hist = build_history(events)
        histories[name] = hist
        if hist.empty:
            print("(no data)")
        else:
            cols = ["record_id", "version", "ts"] + [c for c in hist.columns if c not in ("record_id", "version", "ts")]
            print(format_dataframe_for_display(hist[cols]))

    joined, join_key = build_denorm_join(histories)
    print("\n===== Denormalized joined historical view =====")
    if joined.empty:
        print("(not available due to missing join keys or empty inputs)")
    else:
        display_cols = ['account_id', 'ts']
        priority_cols = ['account_record_id', 'card_record_id', 'saving_record_id', 
                        'saving_balance', 'card_credit_used', 'account_version', 
                        'card_version', 'saving_version']
        
        for col in priority_cols:
            if col in joined.columns and col not in display_cols:
                display_cols.append(col)
        
        for col in joined.columns:
            if col not in display_cols:
                display_cols.append(col)
        
        print(format_dataframe_for_display(joined[display_cols]))

    print("\n===== Transactions detected (changes in saving balance or card credit_used) =====")
    tx = detect_transactions(joined) if not joined.empty else pd.DataFrame()
    if tx.empty:
        print("(no transactions detected)")
    else:
        print(format_dataframe_for_display(tx))
        print("\n-- Transactions summary --")
        by_type = tx.groupby("type")["delta"].agg(["count"]).rename(columns={"count": "num_events"})
        print(by_type.to_string())
        by_acct = tx.groupby(["account_id", "type"])["delta"].agg(["count", "sum"]).reset_index()
        print("\nBy account_id & type (count, sum of delta):")
        print(format_dataframe_for_display(by_acct))


if __name__ == "__main__":
    main()