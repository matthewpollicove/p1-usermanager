import sys, json, time, asyncio
from pathlib import Path
from typing import Dict, Any

# Third Party
import keyring
import httpx
from PySide6 import QtWidgets, QtCore, QtGui

# --- 0. METADATA ---
APP_NAME = "UserManager"
APP_VERSION = "1.3.1"

# --- 1. CORE API CLIENT & WORKERS ---

class PingOneClient:
    """Client for interacting with PingOne API."""
    def __init__(self, env_id, client_id, client_secret):
        self.env_id = env_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = f"https://api.pingone.com/v1/environments/{env_id}"
        self._token = None
        self._token_expires = 0

    def _get_auth_headers(self, token):
        """Helper method to create authorization headers."""
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async def get_token(self):
        """Retrieve and cache access token for API authentication."""
        now = time.time()
        if self._token and now < self._token_expires:
            return self._token
        auth_url = f"https://auth.pingone.com/{self.env_id}/as/token"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(auth_url, data={"grant_type": "client_credentials"}, auth=(self.client_id, self.client_secret))
                resp.raise_for_status()
                data = resp.json()
                self._token = data.get("access_token")
                self._token_expires = now + data.get("expires_in", 3600) - 60
                return self._token
        except Exception:
            return None

    async def update_user(self, user_id, data):
        """Update a user's information in PingOne."""
        token = await self.get_token()
        if not token:
            raise Exception("Auth Failed. Check credentials.")
        headers = self._get_auth_headers(token)
        update_url = f"{self.base_url}/users/{user_id}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.put(update_url, headers=headers, json=data)
            resp.raise_for_status()
            return resp.json()

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
        except Exception as e:
            self.signals.error.emit(str(e))

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

class UserUpdateWorker(QtCore.QRunnable):
    def __init__(self, client, user_id, data):
        super().__init__()
        self.client, self.user_id, self.data, self.signals = client, user_id, data, WorkerSignals()
    @QtCore.Slot()
    def run(self): asyncio.run(self.execute())
    async def execute(self):
        try:
            result = await self.client.update_user(self.user_id, self.data)
            self.signals.finished.emit({"updated": True, "user": result})
        except Exception as e:
            self.signals.error.emit(str(e))

# --- 2. MAIN WINDOW ---

class EditUserDialog(QtWidgets.QDialog):
    """Dialog for editing user information."""
    def __init__(self, user_data, pop_map, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit User")
        self.setModal(True)
        layout = QtWidgets.QFormLayout(self)
        
        self.username = QtWidgets.QLineEdit(user_data.get('username', ''))
        self.email = QtWidgets.QLineEdit(user_data.get('email', ''))
        self.first_name = QtWidgets.QLineEdit(user_data.get('name', {}).get('given', ''))
        self.last_name = QtWidgets.QLineEdit(user_data.get('name', {}).get('family', ''))
        self.phone = QtWidgets.QLineEdit()
        phones = user_data.get('phoneNumbers', [])
        if phones:
            self.phone.setText(phones[0].get('number', ''))
        self.street = QtWidgets.QLineEdit(user_data.get('address', {}).get('streetAddress', ''))
        self.city = QtWidgets.QLineEdit(user_data.get('address', {}).get('locality', ''))
        self.state = QtWidgets.QLineEdit(user_data.get('address', {}).get('region', ''))
        self.zip = QtWidgets.QLineEdit(user_data.get('address', {}).get('postalCode', ''))
        self.country = QtWidgets.QLineEdit(user_data.get('address', {}).get('country', ''))
        self.population = QtWidgets.QComboBox()
        self.population.addItems(list(pop_map.values()))
        current_pop_id = user_data.get('population', {}).get('id', '')
        current_pop_name = pop_map.get(current_pop_id, '')
        self.population.setCurrentText(current_pop_name)
        self.population.setEnabled(False)  # Population not modifiable
        
        layout.addRow("Username:", self.username)
        layout.addRow("Email:", self.email)
        layout.addRow("First Name:", self.first_name)
        layout.addRow("Last Name:", self.last_name)
        layout.addRow("Phone:", self.phone)
        layout.addRow("Street Address:", self.street)
        layout.addRow("City:", self.city)
        layout.addRow("State/Region:", self.state)
        layout.addRow("ZIP/Postal Code:", self.zip)
        layout.addRow("Country:", self.country)
        layout.addRow("Population:", self.population)
        
        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)
        
        self.user_data = user_data
        self.pop_map = pop_map
    
    def get_data(self):
        """Return the updated user data from the dialog."""
        pop_name = self.population.currentText()
        pop_id = next((k for k, v in self.pop_map.items() if v == pop_name), '')
        data = {
            "username": self.username.text(),
            "email": self.email.text(),
            "name": {"given": self.first_name.text(), "family": self.last_name.text()},
            "population": {"id": pop_id}
        }
        if self.phone.text():
            data["phoneNumbers"] = [{"number": self.phone.text(), "type": "mobile"}]  # Assume mobile
        address = {}
        if self.street.text():
            address["streetAddress"] = self.street.text()
        if self.city.text():
            address["locality"] = self.city.text()
        if self.state.text():
            address["region"] = self.state.text()
        if self.zip.text():
            address["postalCode"] = self.zip.text()
        if self.country.text():
            address["country"] = self.country.text()
        if address:
            data["address"] = address
        return data

class ColumnSelectDialog(QtWidgets.QDialog):
    """Dialog for selecting which columns to display in the user table."""
    def __init__(self, all_columns, selected, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Columns")
        layout = QtWidgets.QHBoxLayout(self)
        self.checkboxes = {}
        sorted_cols = sorted(all_columns)
        mid = len(sorted_cols) // 2
        
        for col_list in [sorted_cols[:mid], sorted_cols[mid:]]:
            col_layout = QtWidgets.QVBoxLayout()
            for col in col_list:
                cb = QtWidgets.QCheckBox(col)
                cb.setChecked(col in selected)
                if col == 'id':
                    cb.setEnabled(False)
                col_layout.addWidget(cb)
                self.checkboxes[col] = cb
            col_layout.addStretch()
            layout.addLayout(col_layout)
        
        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
    
    def get_selected(self):
        """Return the list of selected column names."""
        return [col for col, cb in self.checkboxes.items() if cb.isChecked()]

class JSONViewDialog(QtWidgets.QDialog):
    """Dialog for viewing and optionally editing JSON content."""
    def __init__(self, data, editable, parent, user_id, col_name):
        super().__init__(parent)
        self.setWindowTitle("JSON Content")
        self.user_id = user_id
        self.col_name = col_name
        self.parent = parent
        layout = QtWidgets.QVBoxLayout(self)
        self.text = QtWidgets.QTextEdit()
        self.text.setPlainText(json.dumps(data, indent=2))
        if not editable:
            self.text.setReadOnly(True)
        layout.addWidget(self.text)
        buttons = QtWidgets.QHBoxLayout()
        if editable:
            save_btn = QtWidgets.QPushButton("Save")
            save_btn.clicked.connect(self.save_changes)
            buttons.addWidget(save_btn)
        close_btn = QtWidgets.QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        buttons.addWidget(close_btn)
        layout.addLayout(buttons)
    
    def save_changes(self):
        """Save the edited JSON back to the user data."""
        if not self.user_id:
            QtWidgets.QMessageBox.warning(self, "Error", "No user selected for saving.")
            return
        try:
            new_data = json.loads(self.text.toPlainText())
            self.parent.update_user_field(self.user_id, self.col_name, new_data)
            QtWidgets.QMessageBox.information(self, "Saved", "JSON updated successfully.")
        except json.JSONDecodeError:
            QtWidgets.QMessageBox.warning(self, "Error", "Invalid JSON format.")

class MainWindow(QtWidgets.QMainWindow):
    """Main application window for UserManager."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} - v{APP_VERSION}")
        self.setMinimumSize(1200, 800)
        self.threadpool = QtCore.QThreadPool()
        self.config_file, self.users_cache, self.pop_map = Path("profiles.json"), [], {}
        self.columns = []
        self.default_columns = ['username', 'name.given', 'name.family', 'population.name', 'id']
        self.selected_columns = self.default_columns.copy()
        self.all_columns = set()
        self.json_editing_enabled = False
        self.use_friendly_names = True
        self.column_widths = {}
        self.friendly_names = {
            'username': 'Username',
            'name.given': 'First Name',
            'name.family': 'Last Name',
            'email': 'Email',
            'phoneNumbers': 'Phone',
            'population.name': 'Population',
            'id': 'UUID',
            'name': 'Name',
            'address': 'Address',
            # Add more as needed
        }
        self.init_ui()
        self.load_profiles_from_disk()

    def init_ui(self):
        self.tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self.tabs)
        
        # Menu Bar
        menubar = self.menuBar()
        settings_menu = menubar.addMenu("Settings")
        self.enable_json_edit_action = settings_menu.addAction("Enable JSON Editing")
        self.enable_json_edit_action.setCheckable(True)
        self.enable_json_edit_action.setChecked(False)
        self.enable_json_edit_action.triggered.connect(self.toggle_json_editing)
        self.use_friendly_names_action = settings_menu.addAction("Use Friendly Column Names")
        self.use_friendly_names_action.setCheckable(True)
        self.use_friendly_names_action.setChecked(True)
        self.use_friendly_names_action.triggered.connect(self.toggle_friendly_names)
        self.revert_columns_action = settings_menu.addAction("Revert to Default Columns")
        self.revert_columns_action.triggered.connect(self.revert_to_default_columns)
        help_menu = menubar.addMenu("Help")
        config_help_action = help_menu.addAction("Configuration Help")
        config_help_action.triggered.connect(self.show_config_help)
        user_help_action = help_menu.addAction("User Management Help")
        user_help_action.triggered.connect(self.show_user_help)
        
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
        btn_columns = QtWidgets.QPushButton("Columns")
        btn_columns.clicked.connect(self.select_columns)
        toolbar.addWidget(btn_columns)
        btn_save_layout = QtWidgets.QPushButton("Save Layout")
        btn_save_layout.clicked.connect(self.save_columns_to_config)
        toolbar.addWidget(btn_save_layout)
        
        self.u_table = QtWidgets.QTableWidget(0, 0)
        self.u_table.setHorizontalHeaderLabels([])
        self.u_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.u_table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.u_table.setSortingEnabled(True)
        self.u_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Interactive)
        self.u_table.horizontalHeader().setStretchLastSection(False)
        self.u_table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.u_table.customContextMenuRequested.connect(self.show_context_menu)
        self.u_table.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.u_table.horizontalHeader().setSectionsMovable(True)
        self.u_table.horizontalHeader().sectionMoved.connect(self.on_column_moved)
        self.u_table.horizontalHeader().sectionResized.connect(self.on_column_resized)
        
        self.prog = QtWidgets.QProgressBar(); self.prog.hide()
        user_lay.addLayout(toolbar); user_lay.addWidget(self.prog); user_lay.addWidget(self.u_table)
        self.status_label = QtWidgets.QLabel("Ready")
        user_lay.addWidget(self.status_label)
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
            self.selected_columns = p[name].get("columns", self.default_columns.copy())
            self.column_widths = p[name].get("column_widths", {})

    def save_current_profile(self):
        name, ok = QtWidgets.QInputDialog.getText(self, "Save Profile", "Name:")
        if ok and name:
            p = self._read_config(); p[name] = {"env_id": self.env_id.text(), "cl_id": self.cl_id.text()}
            p[name]["columns"] = self.selected_columns
            p[name]["column_widths"] = self.column_widths
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
        
        self.all_columns = self._get_all_columns(self.users_cache)
        self.columns = [c for c in self.selected_columns if c in self.all_columns]
        self.selected_columns = self.columns.copy()
        self.save_columns_to_config()
        self.u_table.setColumnCount(len(self.columns))
        self.u_table.setHorizontalHeaderLabels(self._get_column_labels())
        
        self.u_table.setRowCount(0)
        for u in self.users_cache:
            r = self.u_table.rowCount(); self.u_table.insertRow(r)
            for c, col in enumerate(self.columns):
                value = self._get_value(u, col)
                item = QtWidgets.QTableWidgetItem(str(value))
                item.setData(QtCore.Qt.UserRole, value)
                self.u_table.setItem(r, c, item)
        self.u_table.setSortingEnabled(True)
        self.status_label.setText(f"Loaded {data['user_count']} users, {data['pop_count']} populations")

    def _get_all_columns(self, users):
        """Get list of all available columns based on populated attributes in users."""
        all_keys = set()
        for u in users:
            self._collect_keys(u, '', all_keys)
        # Filter to only include keys that have at least one non-empty value
        populated_keys = set()
        for u in users:
            for key in all_keys:
                if self._get_value(u, key).strip():
                    populated_keys.add(key)
        # Replace population.id with population.name for display
        if 'population.id' in populated_keys:
            populated_keys.discard('population.id')
            populated_keys.add('population.name')
        return sorted(populated_keys)

    def _collect_keys(self, obj, prefix, keys, depth=0, max_depth=3):
        """Recursively collect keys from dict/object."""
        if depth > max_depth:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                full_key = f"{prefix}.{k}" if prefix else k
                keys.add(full_key)
                self._collect_keys(v, full_key, keys, depth + 1, max_depth)
        elif isinstance(obj, list):
            for item in obj[:5]:  # limit to first 5 items to avoid large lists
                self._collect_keys(item, prefix, keys, depth + 1, max_depth)

    def _get_value(self, user, key):
        """Get value from user dict using dot notation."""
        parts = key.split('.')
        current = user
        try:
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part, '')
                else:
                    return ''
            if key == 'population.id':
                return self.pop_map.get(current, current) if current else ''
            elif key == 'population.name':
                p_id = user.get('population', {}).get('id', '')
                return self.pop_map.get(p_id, p_id)
            return str(current) if current else ''
        except:
            return ''

    def delete_selected_users(self):
        rows = self.u_table.selectionModel().selectedRows()
        if not rows: return
        id_col = self.columns.index('id') if 'id' in self.columns else -1
        if id_col == -1: return
        uids = [self.u_table.item(r.row(), id_col).text() for r in rows]
        if QtWidgets.QMessageBox.question(self, "Delete", f"Delete {len(uids)} users?") == QtWidgets.QMessageBox.Yes:
            client = PingOneClient(self.env_id.text(), self.cl_id.text(), self.cl_sec.text())
            self.prog.show()
            w = BulkDeleteWorker(client, uids)
            w.signals.finished.connect(lambda r: (self.prog.hide(), self.refresh_users(), self.status_label.setText(f"Deleted {len(uids)} users")))
            self.threadpool.start(w)

    def filter_table(self):
        txt = self.search_bar.text().lower()
        for i in range(self.u_table.rowCount()):
            match = any(txt in (self.u_table.item(i, j).text() or "").lower() for j in range(self.u_table.columnCount()))
            self.u_table.setRowHidden(i, not match)

    def show_context_menu(self, position):
        menu = QtWidgets.QMenu()
        edit_action = menu.addAction("Edit")
        delete_action = menu.addAction("Delete Selected")
        action = menu.exec(self.u_table.mapToGlobal(position))
        if action == edit_action:
            self.edit_user()
        elif action == delete_action:
            self.delete_selected_users()

    def edit_user(self):
        selected = self.u_table.selectionModel().selectedRows()
        if not selected:
            return
        row = selected[0].row()
        id_col = self.columns.index('id') if 'id' in self.columns else -1
        if id_col == -1: return
        user_id = self.u_table.item(row, id_col).text()
        user_data = next((u for u in self.users_cache if u['id'] == user_id), None)
        if not user_data:
            return
        dialog = EditUserDialog(user_data, self.pop_map, self)
        if dialog.exec() == QtWidgets.QDialog.Accepted:
            new_data = dialog.get_data()
            if QtWidgets.QMessageBox.question(self, "Confirm Update", "Are you sure you want to update this user?") == QtWidgets.QMessageBox.Yes:
                client = PingOneClient(self.env_id.text(), self.cl_id.text(), self.cl_sec.text())
                self.prog.show()
                worker = UserUpdateWorker(client, user_id, new_data)
                worker.signals.finished.connect(lambda r: (self.prog.hide(), self.refresh_users(), self.status_label.setText("User updated")))
                worker.signals.error.connect(lambda m: (self.prog.hide(), QtWidgets.QMessageBox.critical(self, "Error", m)))
                self.threadpool.start(worker)

    def toggle_json_editing(self):
        """Toggle JSON editing mode."""
        self.json_editing_enabled = self.enable_json_edit_action.isChecked()

    def toggle_friendly_names(self):
        """Toggle between friendly names and attribute names for columns."""
        self.use_friendly_names = self.use_friendly_names_action.isChecked()
        self.refresh_table_headers()

    def revert_to_default_columns(self):
        """Revert selected columns to default."""
        self.selected_columns = self.default_columns.copy()
        self.save_columns_to_config()
        self.refresh_table()
        self.status_label.setText("Reverted to default columns")

    def _apply_column_widths(self):
        """Apply saved column widths to the table."""
        for c, col in enumerate(self.columns):
            if col in self.column_widths:
                self.u_table.setColumnWidth(c, self.column_widths[col])

    def refresh_table_headers(self):
        """Refresh only the table headers."""
        self.u_table.setHorizontalHeaderLabels(self._get_column_labels())
        """Update a specific field of a user via API."""
        user = next((u for u in self.users_cache if u['id'] == user_id), None)
        if user:
            user[col_name] = new_data
            client = PingOneClient(self.env_id.text(), self.cl_id.text(), self.cl_sec.text())
            self.prog.show()
            worker = UserUpdateWorker(client, user_id, user)
            worker.signals.finished.connect(lambda r: (self.prog.hide(), self.refresh_users(), self.status_label.setText("User field updated")))
            worker.signals.error.connect(lambda m: (self.prog.hide(), QtWidgets.QMessageBox.critical(self, "Error", m)))
            self.threadpool.start(worker)

    def show_config_help(self):
        help_text = """
Configuration Tab Help:

To make a connection to PingOne:

1. Obtain your PingOne Environment ID, Client ID, and Client Secret from your PingOne admin console.

2. Select or create a profile name in the "Active Profile" dropdown.

3. Enter the Environment ID in the "Env ID" field.

4. Enter the Client ID in the "Client ID" field.

5. Enter the Client Secret in the "Secret" field (it will be stored securely using keyring).

6. Click "Save Profile" to save the credentials. This also saves the selected columns for the profile.

7. Click "Connect & Sync" to test the connection and load users.

Note: Credentials and column preferences (selection and order) are stored securely and associated with the profile name.
"""
        QtWidgets.QMessageBox.information(self, "Configuration Help", help_text)

    def show_user_help(self):
        help_text = """
User Management Tab Help:

Available Options:

- Refresh: Reloads the user list from PingOne.

- Delete Selected: Deletes all currently selected users from the table and PingOne.

- Columns: Opens a dialog to select which columns to display in the table. Selected columns are saved per profile.

- Save Layout: Manually saves the current column selection and order to the active profile.

- Filter: Type in the search box to filter users by any column (username, email, name, population).

- Right-click on users:
  - Edit: Opens a dialog to edit the first selected user's details (username, email, first/last name, phone, address). Population is displayed but not modifiable. A confirmation dialog will appear before saving changes.
  - Delete Selected: Deletes all currently selected users.

- Double-click on cells:
  - Double-click on UUID (ID) to edit the user.
  - Double-click on an email cell to prompt opening the email client.
  - Double-click on JSON-formatted attributes (e.g., name, address, phoneNumbers) to view/edit the JSON in a separate window.

- Settings Menu:
  - Enable JSON Editing: Toggle to allow editing of JSON content in double-click dialogs.
  - Use Friendly Column Names: Toggle between user-friendly names and raw attribute names for column headers.
  - Revert to Default Columns: Reset column selection to the default set.

- Table Features:
  - Click column headers to sort.
  - Drag column boundaries to adjust widths (widths are saved per profile).
  - Drag column headers to reorder columns (order is saved per profile).
  - Select multiple rows with Ctrl+Click or Shift+Click.
  - The 'id' column is always available but cannot be deselected.

Note: All operations require valid credentials and will show progress. Status updates are displayed at the bottom of the screen.
"""
        QtWidgets.QMessageBox.information(self, "User Management Help", help_text)

    def select_columns(self):
        """Open the column selection dialog to choose which columns to display."""
        if not self.all_columns:
            QtWidgets.QMessageBox.information(self, "Info", "Load users first to see available columns.")
            return
        dialog = ColumnSelectDialog(self.all_columns, self.selected_columns, self)
        if dialog.exec() == QtWidgets.QDialog.Accepted:
            self.selected_columns = dialog.get_selected()
            self.save_columns_to_config()
            self.refresh_table()
            self.status_label.setText("Column selection updated")

    def save_columns_to_config(self):
        """Save the selected columns to the current profile's configuration."""
        name = self.profile_list.currentText()
        if not name:
            return
        p = self._read_config()
        if name in p:
            p[name]["columns"] = self.selected_columns
            with open(self.config_file, 'w') as f:
                json.dump(p, f, indent=4)

    def refresh_table(self):
        """Refresh the user table with the currently selected columns."""
        if not self.users_cache:
            return
        self.columns = [c for c in self.selected_columns if c in self.all_columns]
        self.selected_columns = self.columns.copy()
        self.save_columns_to_config()
        self.u_table.setColumnCount(len(self.columns))
        self.u_table.setHorizontalHeaderLabels(self._get_column_labels())
        self.u_table.setRowCount(0)
        for u in self.users_cache:
            r = self.u_table.rowCount()
            self.u_table.insertRow(r)
            for c, col in enumerate(self.columns):
                value = self._get_value(u, col)
                item = QtWidgets.QTableWidgetItem(str(value))
                item.setData(QtCore.Qt.UserRole, value)
                self.u_table.setItem(r, c, item)
        self.u_table.setSortingEnabled(True)
        self._apply_column_widths()

    def on_item_double_clicked(self, item):
        """Handle double-click on table items: edit on ID, email, JSON for name/address."""
        row = item.row()
        col = item.column()
        col_name = self.columns[col]
        id_col = self.columns.index('id') if 'id' in self.columns else -1
        if id_col == -1: return
        user_id = self.u_table.item(row, id_col).text()
        if col_name == 'id':
            self.u_table.selectRow(row)
            self.edit_user()
        elif col_name == 'email':
            email = item.text()
            url = f"mailto:{email}"
            if QtWidgets.QMessageBox.question(self, "Open Email", f"Compose email to {email}?") == QtWidgets.QMessageBox.Yes:
                QtGui.QDesktopServices.openUrl(QtCore.QUrl(url))
        data = item.data(QtCore.Qt.UserRole)
        if isinstance(data, (dict, list)) or col_name in ['name', 'address']:
            dialog = JSONViewDialog(data, self.json_editing_enabled, self, user_id, col_name)
            dialog.exec()

    def on_column_moved(self, logicalIndex, oldVisualIndex, newVisualIndex):
        """Update the selected columns order after user reorders table columns."""
        self.selected_columns = [self.columns[self.u_table.horizontalHeader().visualIndex(i)] for i in range(len(self.columns))]
        self.save_columns_to_config()
        self.status_label.setText("Column order updated")

    def on_column_resized(self, logicalIndex, oldSize, newSize):
        """Save column width when resized."""
        if logicalIndex < len(self.columns):
            col_name = self.columns[logicalIndex]
            self.column_widths[col_name] = newSize
            self.save_columns_to_config()
            self.status_label.setText(f"Column width updated: {col_name}")

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow(); window.show(); sys.exit(app.exec())