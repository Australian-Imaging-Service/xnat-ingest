import flet as ft
from pathlib import Path

from viewer.models import HomeModel
from viewer.views import HomeView
from viewer.controllers import HomeController
from viewer.core.module import Module
from viewer.core.app import Application
from viewer import config

app = Application(
    title=config.TITLE,
    window_width=config.WINDOW_WIDTH,
    window_height=config.WINDOW_HEIGHT,
    window_min_width=config.WINDOW_MIN_WIDTH,
    window_min_height=config.WINDOW_MIN_HEIGHT,
)

# bind the database
current_path = Path(__file__).parent.joinpath("db")
db_path = current_path.joinpath("app.db").absolute()
app.bind_database(f"sqlite:///{db_path}")

# home page
home_page = Module(
    model_class=HomeModel,
    view_class=HomeView,
    controller_class=HomeController,
)

app.add_route("/", home_page)

app.run()
