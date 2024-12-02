import flet as ft
from flet_core import RouteChangeEvent, TemplateRoute

from .module import Module
from .route import Route


class Application:
    def __init__(
        self,
        title: str = "",
        window_width: int = 1600,
        window_height: int = 900,
        window_min_width: int = 1600,
        window_min_height: int = 900,
    ):
        self.title: str = title
        self.window_width = window_width
        self.window_height = window_height
        self.window_min_width = window_min_width
        self.window_min_height = window_min_height

        self.__connection_string = ""
        self.__page: ft.Page = None
        self.__routes: list[Route] = []

    def add_route(self, path: str, module: Module) -> None:
        module.model.app = self
        module.view.app = self
        module.controller.app = self

        module.model.bind_database(self.__connection_string)

        # add the new route
        route = Route(path=path, module=module)
        self.__routes.append(route)

    def run(self):
        ft.app(target=self.__build)

    def goto(self, path: str):
        self.__page.go(path)

    def refresh(self, controls: list[ft.Control] = None) -> None:

        if self.__page is not None:
            if controls is None:
                self.__page.update()
            else:
                self.__page.update(*controls)

    def open_dialog(self, dlg: ft.AlertDialog) -> None:
        self.__page.dialog = dlg
        dlg.open = True
        self.refresh()

    def close_dialog(self, e: ft.ControlEvent) -> None:
        assert isinstance(e, ft.ControlEvent)

        self.__page.close_dialog()

    def close_application(self, e: ft.ControlEvent) -> None:
        assert isinstance(e, ft.ControlEvent)
        self.__page.window_close()

    def bind_database(self, connection_string: str):
        self.__connection_string = connection_string

    def change_theme_mode(self, mode: ft.ThemeMode) -> None:
        self.__page.theme_mode = mode
        self.refresh()

    def __build(self, page: ft.Page) -> None:
        self.__page = page
        self.__page.title = self.title

        self.__page.window.width = self.window_width
        self.__page.window.height = self.window_height
        self.__page.scroll = ft.ScrollMode.AUTO

        self.__page.window.min_width = self.window_min_width
        self.__page.window.min_height = self.window_min_height

        self.__page.on_route_change = self.__route_change
        self.__page.go(self.__page.route)

    @staticmethod
    def __inject_params(path: str, route: TemplateRoute) -> list[str]:
        params = []
        elements = path.split("/")

        for elem in elements:
            if elem.startswith(":"):
                params.append(route.__getattribute__(elem[1:]))

        return params

    def __route_change(self, e: RouteChangeEvent) -> None:
        self.__page = e.page
        self.__page.views.clear()

        for route in self.__routes:
            if route.is_dynamic():
                template_route = TemplateRoute(self.__page.route)

                if template_route.match(route.path):
                    params = self.__inject_params(route.path, template_route)
                    self.__page.views.append(route.module.view.build(*params))

            elif route.path == e.page.route:
                self.__page.views.append(route.module.view.build())

        self.refresh()
