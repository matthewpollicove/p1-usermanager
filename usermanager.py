import sys, json, time, asyncio, csv, logging, re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# Third Party
import keyring
import httpx
from PySide6 import QtWidgets, QtCore, QtGui

# --- 0. METADATA ---
APP_NAME = "UserManager"
APP_VERSION = "1.3.1"

# --- 1. CORE API CLIENT & WORKERS ---

class PingOneClient:
    def __init__(self, env_id, client_id, client_secret):
        self.env_id = env_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = f"https://api.pingone.com/v1/environments/{env_id}"

    async def get_token(self):
        auth_url = f"https://auth.pingone.com/{self.env_id}/as/token"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    auth_url, 
                    data={"grant_type": "client_credentials"}, 
                    auth=(self.client_id, self.client_secret)
                )
                resp.raise_for_status()
                return resp.json().get("access_token")
        except Exception: return None

class WorkerSignals(QtCore.QObject):
    finished = QtCore.Signal(dict)
    progress = QtCore.Signal(int, int)
    error = QtCore.Signal(str)

class UserFetchWorker(QtCore.QRunnable):
    def __init__(self, client):
        super().__init__()
        self.client, self.signals = client, WorkerSignals()
    @QtCore.Slot()
    def run(self): asyncio.run(self.execute())
    async def execute(self):
        try:
            token = await self.client.get_token()
            if not token: 
                self.signals.error.emit("Auth Failed. Check credentials.")
                return
            headers = {"Authorization": f"Bearer {token}"}
            async with httpx.AsyncClient() as session:
                p_resp = await session.get(f"{self.client.base_url}/populations", headers=headers)
                pop_map = {p['id']: p['name'] for p in p_resp.json().get('_embedded', {}).get('populations', [])}
                all_users, url = [], f"{self.client.base_url}/users"
                while url:
                    resp = await session.get(url, headers=headers)
                    data = resp.json()
                    all_users.extend(data.get("_embedded", {}).get("users", []))
                    url = data.get("_links", {}).get("next", {}).get("href")
            self.signals.finished.emit({"users": all_users, "pop_map": pop_map, "user_count": len(all_users), "pop_count": len(pop_map)})
        except Exception as e: self.signals.error.emit(str(e))

class BulkDeleteWorker(QtCore.QRunnable):
    def __init__(self, client, user_ids):
        super().__init__()
        self.client, self.user_ids, self.signals = client, user_ids, WorkerSignals()
    @QtCore.Slot()
    def run(self): asyncio.run(self.execute())
    async def execute(self):
        token = await self.client.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        success = 0
        async with httpx.AsyncClient() as session:
            for i, uid in enumerate(self.user_ids):
                try:
                    await session.delete(f"{self.client.base_url}/users/{uid}", headers=headers)
                    success += 1
                except: pass
                self.signals.progress.emit(i + 1, len(self.user_ids))
        self.signals.finished.emit({"deleted": success, "total": len(self.user_ids)})

# --- 2. MAIN WINDOW ---

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} - v{APP_VERSION}")
        self.setMinimumSize(1200, 800)
        self.threadpool = QtCore.QThreadPool()
        self.config_file, self.users_cache, self.pop_map = Path("profiles.json"), [], {}
        self.init_ui()
        self.load_profiles_from_disk()

    def init_ui(self):
        self.tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self.tabs)
        
        # --- Config Tab ---
        env_tab = QtWidgets.QWidget(); env_lay = QtWidgets.QVBoxLayout(env_tab)
        prof_group = QtWidgets.QGroupBox("Profiles")
        prof_form = QtWidgets.QFormLayout(prof_group)
        self.profile_list = QtWidgets.QComboBox()
        self.profile_list.currentIndexChanged.connect(self.load_selected_profile)
        prof_form.addRow("Active Profile:", self.profile_list)
        
        cred_group = QtWidgets.QGroupBox("Credentials")
        cred_form = QtWidgets.QFormLayout(cred_group)
        self.env_id, self.cl_id = QtWidgets.QLineEdit(), QtWidgets.QLineEdit()
        self.cl_sec = QtWidgets.QLineEdit(); self.cl_sec.setEchoMode(QtWidgets.QLineEdit.Password)
        btn_save = QtWidgets.QPushButton("Save Profile"); btn_save.clicked.connect(self.save_current_profile)
        btn_sync = QtWidgets.QPushButton("Connect & Sync"); btn_sync.clicked.connect(self.refresh_users)
        cred_form.addRow("Env ID:", self.env_id); cred_form.addRow("Client ID:", self.cl_id)
        cred_form.addRow("Secret:", self.cl_sec); cred_form.addRow(btn_save); cred_form.addRow(btn_sync)
        
        self.lbl_stats = QtWidgets.QLabel("Users: -- | Populations: --")
        env_lay.addWidget(prof_group); env_lay.addWidget(cred_group); env_lay.addWidget(self.lbl_stats); env_lay.addStretch()

        # --- Users Tab ---
        user_tab = QtWidgets.QWidget(); user_lay = QtWidgets.QVBoxLayout(user_tab)
        toolbar = QtWidgets.QHBoxLayout()
        btn_reload = QtWidgets.QPushButton("🔄 Refresh"); btn_reload.clicked.connect(self.refresh_users)
        btn_del = QtWidgets.QPushButton("🗑 Delete Selected")
        btn_del.setStyleSheet("background-color: #d9534f; color: white;")
        btn_del.clicked.connect(self.delete_selected_users)
        
        self.search_bar = QtWidgets.QLineEdit(); self.search_bar.setPlaceholderText("Filter...")
        self.search_bar.textChanged.connect(self.filter_table)
        
        toolbar.addWidget(btn_reload); toolbar.addWidget(btn_del); toolbar.addWidget(self.search_bar)
        
        self.u_table = QtWidgets.QTableWidget(0, 6)
        self.u_table.setHorizontalHeaderLabels(["Username", "Email", "First", "Last", "Population", "ID"])
        self.u_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.u_table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.u_table.setSortingEnabled(True)
        self.u_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        
        self.prog = QtWidgets.QProgressBar(); self.prog.hide()
        user_lay.addLayout(toolbar); user_lay.addWidget(self.prog); user_lay.addWidget(self.u_table)
        self.tabs.addTab(env_tab, "Configuration"); self.tabs.addTab(user_tab, "User Management")

    # --- Profile Methods ---
    def _read_config(self):
        if self.config_file.exists():
            with open(self.config_file, 'r') as f: return json.load(f)
        return {}

    def load_profiles_from_disk(self):
        self.profile_list.blockSignals(True); self.profile_list.clear()
        self.profile_list.addItems(list(self._read_config().keys()))
        self.profile_list.blockSignals(False)
        if self.profile_list.count() > 0: self.load_selected_profile()

    def load_selected_profile(self):
        name = self.profile_list.currentText()
        p = self._read_config()
        if name in p:
            self.env_id.setText(p[name].get("env_id", ""))
            self.cl_id.setText(p[name].get("cl_id", ""))
            self.cl_sec.setText(keyring.get_password("PingOneUM", name) or "")

    def save_current_profile(self):
        name, ok = QtWidgets.QInputDialog.getText(self, "Save Profile", "Name:")
        if ok and name:
            p = self._read_config(); p[name] = {"env_id": self.env_id.text(), "cl_id": self.cl_id.text()}
            with open(self.config_file, 'w') as f: json.dump(p, f, indent=4)
            keyring.set_password("PingOneUM", name, self.cl_sec.text()); self.load_profiles_from_disk()

    # --- THE MISSING SLOT ---
    def refresh_users(self):
        """Fixes the AttributeError by providing the reload function."""
        client = PingOneClient(self.env_id.text(), self.cl_id.text(), self.cl_sec.text())
        self.prog.show(); self.prog.setRange(0, 0)
        worker = UserFetchWorker(client)
        worker.signals.finished.connect(self.on_fetch_success)
        worker.signals.error.connect(lambda m: (self.prog.hide(), QtWidgets.QMessageBox.critical(self, "Error", m)))
        self.threadpool.start(worker)

    def on_fetch_success(self, data):
        self.prog.hide(); self.u_table.setSortingEnabled(False)
        self.lbl_stats.setText(f"Users: {data['user_count']} | Populations: {data['pop_count']}")
        self.pop_map, self.users_cache = data['pop_map'], data['users']
        self.u_table.setRowCount(0)
        for u in self.users_cache:
            r = self.u_table.rowCount(); self.u_table.insertRow(r)
            self.u_table.setItem(r, 0, QtWidgets.QTableWidgetItem(u.get('username','')))
            self.u_table.setItem(r, 1, QtWidgets.QTableWidgetItem(u.get('email','')))
            name = u.get('name', {}); self.u_table.setItem(r, 2, QtWidgets.QTableWidgetItem(name.get('given','')))
            self.u_table.setItem(r, 3, QtWidgets.QTableWidgetItem(name.get('family','')))
            p_id = u.get('population', {}).get('id','')
            self.u_table.setItem(r, 4, QtWidgets.QTableWidgetItem(self.pop_map.get(p_id, p_id)))
            self.u_table.setItem(r, 5, QtWidgets.QTableWidgetItem(u.get('id','')))
        self.u_table.setSortingEnabled(True)

    def delete_selected_users(self):
        rows = self.u_table.selectionModel().selectedRows()
        if not rows: return
        uids = [self.u_table.item(r.row(), 5).text() for r in rows]
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete {len(uids)} users?") == QtWidgets.QMessageBox.Yes:
            client = PingOneClient(self.env_id.text(), self.cl_id.text(), self.cl_sec.text())
            self.prog.show()
            w = BulkDeleteWorker(client, uids)
            w.signals.progress.connect(lambda c, t: (self.prog.setRange(0, t), self.prog.setValue(c)))
            w.signals.finished.connect(lambda r: (self.prog.hide(), self.refresh_users()))
            self.threadpool.start(w)

    def filter_table(self):
        txt = self.search_bar.text().lower()
        for i in range(self.u_table.rowCount()):
            match = any(txt in (self.u_table.item(i, j).text() or "").lower() for j in range(self.u_table.columnCount()))
            self.u_table.setRowHidden(i, not match)

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(); window.show(); sys.exit(app.exec())