import flet as ft
from viewer.core.controller import BaseController
import os
from odm.jcampdx import load_jcampdx
from viewer.utils.data_type import determine_file_type_magic
from viewer import config
from viewer.utils.constants import JCAMPDX
import json


class HomeController(BaseController):
    def __init__(self):
        super().__init__()
        self.root_path = None
        self.current_path = None
        self.file_list = []
        self.meta_data = {}
        self.checked_metadata = {}

    def on_directory_open(self, e: ft.FilePickerResultEvent):
        self.root_path = e.path
        self.current_path = e.path
        self.view.current_path.value = self.current_path

        self.file_list = self.get_directory_contents()
        self.view.update_file_list_controls()

        self.refresh()

    def on_file_click(self, e, path):
        self.current_path = path
        file_type = determine_file_type_magic(path)
        if file_type == JCAMPDX:
            self.meta_data = load_jcampdx(path)
            self.view.show_jcampdx_metadata(path.replace(self.root_path, ""))
            self.refresh()

    def on_back_click(self, e):
        self.current_path = os.path.dirname(self.current_path)
        self.__update_file_list()

    def on_directory_click(self, e, path):
        self.current_path = path
        self.__update_file_list()

    def __update_file_list(
        self,
    ):
        self.view.current_path.value = self.current_path
        self.file_list = self.get_directory_contents()
        self.view.update_file_list_controls()
        self.refresh()

    def get_directory_contents(self):
        """Get contents of current directory"""
        try:
            items = sorted(os.listdir(self.current_path))
            return [
                (
                    os.path.join(self.current_path, item),
                    determine_file_type_magic(os.path.join(self.current_path, item)),
                )
                for item in items
            ]
        except FileNotFoundError:
            return ["Directory not found"]
        except PermissionError:
            return ["Permission denied"]

    def on_metadata_checked(self, e, path, key, value):
        path = path.replace(self.root_path, "")
        if (path, key) not in self.checked_metadata:
            self.checked_metadata[(path, key)] = value
            self.view.update_selected_metadata_controls()
            self.refresh()
        else:
            del self.checked_metadata[(path, key)]
            self.view.update_selected_metadata_controls()
            self.refresh()

    def on_metadata_deleted(self, e, path, key):
        del self.checked_metadata[(path, key)]
        self.view.update_selected_metadata_controls()
        self.view.show_jcampdx_metadata(path)
        self.refresh()

    def on_export_to_json(self, e):
        pass

    def on_save_as_template(self, e):
        if e.path:
            with open(e.path, "w") as f:
                data = self.__metadata_to_json()
                print(data)
                json.dump(data, f)

    def __metadata_to_json(self):
        result = {}
        for (path, key), _ in self.checked_metadata.items():
            if path not in result:
                result[path] = []
            result[path].append(key)
        return result

    def on_template_load(self, e):
        template_json = e.files[0].path

        if template_json:
            with open(template_json, "r") as f:
                data = json.load(f)
                for path, keys in data.items():
                    jcampdx_file_path = "".join([self.root_path, path])
                    __meta_data = load_jcampdx(jcampdx_file_path)
                    for key in keys:
                        truncated_path = path.replace(self.root_path, "")
                        print(truncated_path)
                        self.checked_metadata[(truncated_path, key)] = __meta_data[key]

                self.view.update_selected_metadata_controls()
                self.view.show_jcampdx_metadata(self.current_path)
                self.refresh()

    def __json_to_metadata(self, data):
        return {(k, v) for k, v in data.items()}

    def handle_search(self, e):
        """Handle search input changes"""
        search_text = e.control.value
        # TODO: Implement search filtering
        self.view.update()
