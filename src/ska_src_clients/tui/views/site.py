from textual.app import ComposeResult
from textual.widgets import Static, Tabs, Tab, DataTable
from textual.containers import Vertical
from rich.table import Table
from rich.json import JSON

from ska_src_clients.api import SiteAPI


class SiteTUIView(Vertical):
    def _populate_site_detail(self, row):
        try:
            #FIXME: hardcoded indexes
            selected_name = row[2]
            selected_site = next((item for item in self.data if item.get("name") == selected_name),
                                 None)
            site_data = self.api.get_site(selected_site.get("id"))
            self.detail.update(JSON.from_data(site_data))
        except Exception as e:
            self.detail.update(f"[red]Error: {e}[/red]")


    def _populate_sites(self):
        self.info.add_columns(
            "ID",
            "Parent Node Name",
            "Site Name",
            "Description",
            "Latitude",
            "Longitude",
            "Country",
            "Primary Email",
            "Force Disabled?"
        )

        # Populate info table
        data = self.api.list_sites()
        for row in data:
            self.info.add_row(
                str(row.get("id", "")),
                str(row.get("parent_node_name", "")),
                str(row.get("name", "")),
                str(row.get("description", "")),
                str(row.get("latitude", "")),
                str(row.get("longitude", "")),
                str(row.get("country", "")),
                str(row.get("primary_contact_email", "")),
                "✓" if row.get("is_force_disabled") else "✗"
            )

        self.data = data

    def do_update(self, container):
        if not hasattr(self, "api"):
            return
        self.info = container.query_one("#info", DataTable)
        self.detail = container.query_one("#detail", Static)

        self.update_info_pane()

    def set_context(self, session, view):
        self.session = session
        self.api = SiteAPI(session=session)
        self.current_view = view

    def update_detail_pane(self, event: DataTable.RowSelected):
        row = self.info.get_row(event.row_key)
        # Set up detail pane

        # Populate detail pane based on row and view requested
        match self.current_view:
            case "sites":
                self._populate_site_detail(row=row)

    def update_info_pane(self):
        # Set up info table
        self.info.clear()
        self.info.zebra_stripes = True
        self.info.cursor_type = "row"

        # populate info table based on view requested
        match self.current_view:
            case "sites":
                self._populate_sites()

