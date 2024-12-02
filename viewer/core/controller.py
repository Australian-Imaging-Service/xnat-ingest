import flet as ft


class BaseController:
    def __init__(self):
        self.app = None
        self.model = None
        self.view = None

    def goto(self, path: str):
        self.app.goto(path=path)

    def refresh(self, controls: list[ft.Control] = None) -> None:
        self.app.refresh(controls=controls)

    def open_dialog(self, dlg: ft.AlertDialog) -> None:
        self.app.open_dialog(dlg=dlg)

    def close_dialog(self, e: ft.ControlEvent) -> None:
        self.app.close_dialog(e=e)

    def close_application(self, e: ft.ControlEvent) -> None:
        self.app.close_application(e=e)

    def change_theme_mode(self, mode: ft.ThemeMode) -> None:
        self.app.change_theme_mode(mode=mode)
