import os, json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

DEFAULT_ITEMS = ["Булла_Оранж", "Булла_Ред", "Клеймо", "Галун", "Руны", "Ключи", "Кристалл"]

class GSheetWrapper:
    def __init__(self, sheet_id=None):
        self.sheet_id = sheet_id or os.getenv('GSHEET_ID')
        self.client = None
        self.sheet = None
        if self.sheet_id:
            self._authorize()

    def _authorize(self):
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds_env = os.getenv('GOOGLE_CREDENTIALS')
        if creds_env:
            creds = json.loads(creds_env)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
        elif os.path.exists('service_account.json'):
            credentials = ServiceAccountCredentials.from_json_keyfile_name('service_account.json', scope)
        else:
            raise RuntimeError('No Google credentials: set GOOGLE_CREDENTIALS or provide service_account.json')
        self.client = gspread.authorize(credentials)
        self.sheet = self.client.open_by_key(self.sheet_id)

    def ensure_tabs(self):
        required = ['Игроки','История БМ','Аукцион','Отсутствия','Лог','Аналитика']
        existing = [ws.title for ws in self.sheet.worksheets()]
        for name in required:
            if name not in existing:
                self.sheet.add_worksheet(title=name, rows=200, cols=60)
        players = self.sheet.worksheet('Игроки')
        players.update('A1:G1', [['tg_id','telegram','nick','old_nicks','class','current_bm','bm_updated']])
        hist = self.sheet.worksheet('История БМ')
        hist.update('A1:G1', [['tg_id','nick','class','old_bm','new_bm','diff','ts']])
        auction = self.sheet.worksheet('Аукцион')
        if not auction.row_values(1):
            auction.update('A1', [DEFAULT_ITEMS])

    def update_player(self, player):
        ws = self.sheet.worksheet('Игроки')
        ids = [r for r in ws.col_values(1)[1:] if r.strip()]
        idx_map = {int(v): i+2 for i,v in enumerate(ids)} if ids else {}
        row = idx_map.get(player['tg_id'])
        values = [[player['tg_id'], player['telegram'], player['nick'], player.get('old_nicks',''), player.get('class',''), player.get('current_bm',''), player.get('bm_updated','')]]
        if row: ws.update(f'A{row}:G{row}', values)
        else: ws.append_row(values[0])

    def rename_everywhere(self, old_nick, new_nick):
        try:
            ws = self.sheet.worksheet('История БМ'); data = ws.get_all_values()
            if data:
                header = data[0]; nick_idx = header.index('nick') if 'nick' in header else 1
                changed=False
                for i in range(1,len(data)):
                    if len(data[i])>nick_idx and data[i][nick_idx]==old_nick:
                        data[i][nick_idx]=new_nick; changed=True
                if changed: ws.clear(); ws.update('A1', data)
        except: pass
        try:
            ws = self.sheet.worksheet('Отсутствия'); data = ws.get_all_values()
            if data:
                nick_idx = 1; changed=False
                for i in range(1,len(data)):
                    if len(data[i])>nick_idx and data[i][nick_idx]==old_nick:
                        data[i][nick_idx]=new_nick; changed=True
                if changed: ws.clear(); ws.update('A1', data)
        except: pass
        try:
            ws = self.sheet.worksheet('Аукцион'); data = ws.get_all_values()
            if data:
                header = data[0]
                for c in range(len(header)):
                    col = [row[c] if len(row)>c else '' for row in data[1:]]
                    changed=False
                    for r in range(len(col)):
                        if col[r]==old_nick:
                            col[r]=new_nick; changed=True
                    if changed:
                        max_len = max(len(col), len(data)-1)
                        while len(data)-1 < max_len: data.append(['']*len(header))
                        for i in range(max_len): data[i+1][c] = col[i] if i < len(col) else ''
                ws.clear(); ws.update('A1', data)
        except: pass

    def append_bm_history(self, rec):
        ws = self.sheet.worksheet('История БМ')
        ws.append_row([rec['tg_id'], rec['nick'], rec.get('class',''), rec['old_bm'], rec['new_bm'], rec['diff'], rec['ts']])

    def get_auction_matrix(self):
        ws = self.sheet.worksheet('Аукцион')
        return ws.get_all_values(), ws

    def write_auction_matrix(self, ws, matrix):
        ws.clear(); ws.update('A1', matrix)

    def append_absence(self, date, nick, telegram, reason):
        ws = self.sheet.worksheet('Отсутствия')
        ws.append_row([date, nick, telegram, reason])

    def write_log(self, ts, tg_id, nick, action, details):
        ws = self.sheet.worksheet('Лог')
        ws.append_row([ts, tg_id, nick, action, details])
