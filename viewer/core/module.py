from .controller import BaseController
from .model import BaseModel
from .view import BaseView


class Module:
    def __init__(
        self,
        model_class: BaseModel.__class__,
        view_class: BaseView.__class__,
        controller_class: BaseController.__class__,
    ):
        self.model = model_class()
        self.view = view_class()
        self.controller = controller_class()

        self.model.view = self.view
        self.model.controller = self.controller

        self.view.model = self.model
        self.view.controller = self.controller

        self.controller.model = self.model
        self.controller.view = self.view
