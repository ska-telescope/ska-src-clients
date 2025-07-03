import logging

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, DataTable, Static, Input
from textual.containers import Vertical, Horizontal, Container

from ska_src_clients.tui.views.site import SiteTUIView

from ska_src_clients.common.utility import load_config
from ska_src_clients.session.oidc import OIDCSession


class OperApp(App):
    DEFAULT_BINDINGS = [
        (":", "open_command_input", "Command"),
        ("q", "quit", "Quit"),
    ]
    CSS_PATH = "oper.tcss"
    TITLE = "SRCNet Operator"

    # Load views and set active
    LOADED_VIEWS = {
        'sites': (SiteTUIView(), 's', 'load_sites_view'),
    }
    ACTIVE_VIEW_NAME = "sites"

    # Load all bindings
    VIEW_BINDINGS = []
    for name, (view, binding, action) in LOADED_VIEWS.items():
        VIEW_BINDINGS.append(
            (binding, action, name.capitalize()),
        )
    BINDINGS = DEFAULT_BINDINGS + VIEW_BINDINGS

    async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        view = self.LOADED_VIEWS.get(self.ACTIVE_VIEW_NAME)[0]
        if hasattr(view, "update_detail_pane"):
            view.update_detail_pane(event=event)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Input(placeholder="Enter command...", id="command_input", classes="hidden")
        yield Container(
            Vertical(
                DataTable(id="info"),
                Static("", id="detail"),
                id="app"
            ),
            id="main"
        )
        yield Footer()

    async def action_open_command_input(self):
        input_widget = self.query_one("#command_input", Input)
        input_widget.remove_class("hidden")
        input_widget.focus()

    async def action_load_sites_view(self):
        await self.load_view(view_name="sites")

    async def load_view(self, view_name):
        # Setup the view
        view, _, _ = self.LOADED_VIEWS.get(view_name)
        view.set_context(session=self.session, view=view_name)

        # Display the view
        container = self.query_one("#app", Vertical)
        view.do_update(container)

        self.ACTIVE_VIEW_NAME = view_name

    async def on_input_submitted(self, event: Input.Submitted):
        command = event.value.strip().lower()
        event.input.add_class("hidden")
        event.input.value = ""

        if command in self.LOADED_VIEWS.keys():
            await self.load_view(view_name=command)
        else:
            #TODO: handle
            pass

        # Hide input again
        event.input.add_class("hidden")
        event.input.value = ""

    async def on_key(self, event):
        if event.key == "escape":
            input_widget = self.query_one("#command_input", Input)
            input_widget.add_class("hidden")
            input_widget.value = ""

    async def on_mount(self) -> None:
        # Configure logging and session
        logging.basicConfig(
            level=logging.CRITICAL,
            format="%(asctime)s [%(name)s] %(module)10s %(levelname)5s %(process)d\t%(message)s"
        )
        config = load_config()
        if not config:
            logging.critical("No valid config file found.")
            exit(1)

        self.session = OIDCSession(config=config)

        # Load any existing local tokens
        self.session.load_tokens_from_disk()

        # Load active view
        if self.ACTIVE_VIEW_NAME:
            await self.load_view(view_name=self.ACTIVE_VIEW_NAME)

        # Set focus on info datatable otherwise bindings don't appear
        self.set_focus(self.query_one("#info", DataTable))
