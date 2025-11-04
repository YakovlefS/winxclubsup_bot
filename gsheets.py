import json
import gspread
from typing import List, Tuple
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_CREDENTIALS = None

def _creds():
    global GOOGLE_CREDENTIALS
    if GOOGLE_CREDENTIALS is None:
        import os
        GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    if not GOOGLE_CREDENTIALS:
        return None
    data = json.loads(GOOGLE_CREDENTIALS)
    return Credentials.from_service_account_info(data, scopes=SCOPES)

class GSheetWrapper:
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self.gc = None
        self.sheet = None
        creds = _creds()
        if creds:
            self.gc = gspread.authorize(creds)
            self.sheet = self.gc.open_by_key(sheet_id)

    # ---------- Общие вкладки ----------
    def ensure_tabs(self):
        if not self.sheet:
            return
        needed = {"Игроки": ["tg_id","telegram","nick","old_nicks","class","current_bm","bm_updated"],
                  "Аукцион": ["Булла_Ред","Клеймо","Галун"],
                  "Логи": ["ts","tg_id","nick","action","data"],
                  "Отсутствия": ["date","nick","telegram","reason"]}
        existing = {ws.title for ws in self.sheet.worksheets()}
        for name, header in needed.items():
            if name not in existing:
                self.sheet.add_worksheet(title=name, rows=1000, cols=40)
            ws = self.sheet.worksheet(name)
            vals = ws.get_all_values()
            if not vals:
                ws.append_row(header, value_input_option="USER_ENTERED")

    # ---------- Игроки ----------
    def update_player(self, player: dict):
        ws = self.sheet.worksheet("Игроки")
        data = ws.get_all_values()
        header = data[0]
        idx = {h:i for i,h in enumerate(header)}
        row_i = None
        if "tg_id" in idx:
            for i, r in enumerate(data[1:], start=2):
                if len(r) > idx["tg_id"] and r[idx["tg_id"]] == str(player.get("tg_id","")):
                    row_i = i
                    break
        row = [""]*len(header)
        row[idx["tg_id"]] = str(player.get("tg_id","") or "")
        row[idx["telegram"]] = player.get("telegram","") or ""
        row[idx["nick"]] = player.get("nick","") or ""
        row[idx["old_nicks"]] = player.get("old_nicks","") or ""
        row[idx["class"]] = player.get("class","") or ""
        row[idx["current_bm"]] = str(player.get("current_bm","") or "")
        if "bm_updated" in idx:
            row[idx["bm_updated"]] = player.get("bm_updated","") or ""
        if row_i:
            ws.update(f"A{row_i}:{chr(64+len(header))}{row_i}", [row])
        else:
            ws.append_row(row, value_input_option="USER_ENTERED")

    def append_bm_history(self, rec: dict):
        ws = self.sheet.worksheet("Логи")
        ws.append_row([rec.get("ts",""), rec.get("tg_id",""), rec.get("nick",""), "bm_update",
                       f'{rec.get("old_bm","")}->{rec.get("new_bm","")}({rec.get("diff","")})'],
                      value_input_option="USER_ENTERED")

    def write_log(self, ts, tg_id, nick, action, data):
        ws = self.sheet.worksheet("Логи")
        ws.append_row([ts, tg_id, nick, action, data], value_input_option="USER_ENTERED")

    # ---------- Отсутствия ----------
    def append_absence(self, date, nick, telegram, reason):
        ws = self.sheet.worksheet("Отсутствия")
        ws.append_row([date, nick, telegram, reason], value_input_option="USER_ENTERED")

    # ---------- Аукцион ----------
    def get_auction_matrix(self) -> Tuple[List[List[str]], "gspread.Worksheet"]:
        ws = self.sheet.worksheet("Аукцион")
        data = ws.get_all_values()
        return data, ws

    def write_auction_matrix(self, ws, matrix: List[List[str]]):
        rng = f"A1:{gspread.utils.rowcol_to_a1(len(matrix), len(matrix[0]))}"
        ws.update(rng, matrix, value_input_option="USER_ENTERED")

    def rename_everywhere(self, old, new):
        # Простая замена в очередях
        data, ws = self.get_auction_matrix()
        if not data: return
        header = data[0]
        for ci in range(len(header)):
            col = [r[ci] if len(r)>ci else "" for r in data[1:]]
            changed = False
            for i, val in enumerate(col):
                if val == old:
                    col[i] = new
                    changed = True
            if changed:
                max_len = max(len(col), len(data)-1)
                while len(data)-1 < max_len: data.append(['']*len(header))
                for i in range(max_len):
                    data[i+1][ci] = col[i] if i < len(col) else ""
        self.write_auction_matrix(ws, data)

    def list_items(self) -> List[str]:
        data, _ = self.get_auction_matrix()
        return data[0] if data else []

    def add_item(self, name: str) -> bool:
        data, ws = self.get_auction_matrix()
        if not data:
            data = [[name]]
        else:
            if name in data[0]:
                return False
            data[0].append(name)
            for r in range(1, len(data)):
                data[r].append("")
        self.write_auction_matrix(ws, data)
        return True

    def remove_item(self, name: str) -> bool:
        data, ws = self.get_auction_matrix()
        if not data or name not in data[0]:
            return False
        ci = data[0].index(name)
        for r in range(len(data)):
            del data[r][ci]
        self.write_auction_matrix(ws, data)
        return True
