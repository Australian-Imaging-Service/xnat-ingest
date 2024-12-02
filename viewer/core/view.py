import flet as ft


class BaseView:
    def __init__(self):
        self.app = None
        self.model = None
        self.controller = None

    def build(self) -> ft.Control:
        raise NotImplementedError("Subclasses must implement build()")
