import flet as ft
from viewer.core.view import BaseView
import os
from viewer.utils.constants import DIRECTORY
from viewer import config


class HomeView(BaseView):
    def __init__(self):
        super().__init__()

        # menu bar
        self.menu_bar = ft.Ref[ft.MenuBar]()
        self.file_submenu = ft.Ref[ft.SubmenuButton]()
        self.viewer_submenu = ft.Ref[ft.SubmenuButton]()
        self.scheduler_submenu = ft.Ref[ft.SubmenuButton]()
        self.upload_submenu = ft.Ref[ft.SubmenuButton]()
        self.log_submenu = ft.Ref[ft.SubmenuButton]()
        self.help_submenu = ft.Ref[ft.SubmenuButton]()
        self.file_picker = ft.Ref[ft.FilePicker]()
        self.template_loader = ft.Ref[ft.FilePicker]()
        self.view = ft.Ref[ft.View]()

        # metadata viewer
        self.back_button = ft.Ref[ft.ElevatedButton]()
        self.current_path = ft.Ref[ft.Text]()
        self.file_explorer = ft.Ref[ft.ListView]()

        # Metadata table
        self.metadata_list = ft.Ref[ft.ListView]()

        # selected metadata
        self.selected_metadata = ft.Ref[ft.ListView]()
        self.selected_meta_header = ft.Ref[ft.Row]()
        self.selected_meta_content = ft.Ref[ft.Row]()
        self.selected_meta_buttons = ft.Ref[ft.Row]()
        self.template_saver = ft.Ref[ft.FilePicker]()

    def build(self) -> ft.Control:

        self.build_menubar_components()
        self.build_metadata_components()
        self.build_selected_metadata_components()

        self.view = ft.View(
            controls=[
                ft.Row(controls=[self.menu_bar_view]),
                ft.Row(
                    controls=[
                        self.back_button,
                        self.current_path,
                    ]
                ),
                ft.Row(
                    controls=[
                        ft.Container(
                            content=self.file_explorer,
                            expand=1,
                        ),
                        ft.VerticalDivider(width=10),
                        ft.Container(
                            content=self.metadata_list,
                            expand=3,
                        ),
                    ],
                    expand=True,
                ),
                # add a horizontal divider
                ft.Divider(height=10),
                self.template_saver,
                self.selected_meta_header,
                self.selected_meta_content,
                self.selected_meta_buttons,
            ],
        )

        return self.view

    def build_menubar_components(self):
        self.file_picker = ft.FilePicker(
            ref=self.file_picker, on_result=self.controller.on_directory_open
        )
        self.template_loader = ft.FilePicker(
            ref=self.template_loader, on_result=self.controller.on_template_load
        )

        self.file_submenu = ft.SubmenuButton(
            ref=self.file_submenu,
            content=ft.Text("File"),
            controls=[
                ft.MenuItemButton(
                    content=ft.Text("Open Local"),
                    leading=ft.Icon(ft.icons.FOLDER_OPEN),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.file_picker.get_directory_path(
                        initial_directory=config.ROOT_DATA_DIRECTORY
                    ),
                ),
                ft.MenuItemButton(
                    content=ft.Text("Open Remote"),
                    leading=ft.Icon(ft.icons.FOLDER_OPEN),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
                ft.MenuItemButton(
                    content=ft.Text("Export"),
                    leading=ft.Icon(ft.icons.SAVE),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
            ],
        )

        self.viewer_submenu = ft.SubmenuButton(
            ref=self.viewer_submenu,
            content=ft.Text("Viewer"),
            controls=[
                ft.MenuItemButton(
                    content=ft.Text("Load Template"),
                    leading=ft.Icon(ft.icons.OPEN_WITH),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.template_loader.pick_files(
                        allow_multiple=False, allowed_extensions=["json"]
                    ),
                ),
                ft.MenuItemButton(
                    content=ft.Text("Setting"),
                    leading=ft.Icon(ft.icons.SETTINGS),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
            ],
        )
        self.scheduler_submenu = ft.SubmenuButton(
            ref=self.scheduler_submenu,
            content=ft.Text("Scheduler"),
            controls=[
                ft.MenuItemButton(
                    content=ft.Text("Add Schedule"),
                    leading=ft.Icon(ft.icons.ADD_ALARM),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
                ft.MenuItemButton(
                    content=ft.Text("View Schedule"),
                    leading=ft.Icon(ft.icons.CALENDAR_TODAY),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
            ],
        )

        self.upload_submenu = ft.SubmenuButton(
            ref=self.upload_submenu,
            content=ft.Text("Upload"),
            controls=[
                ft.MenuItemButton(
                    content=ft.Text("Add New"),
                    leading=ft.Icon(ft.icons.ADD),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
                ft.MenuItemButton(
                    content=ft.Text("Configurations"),
                    leading=ft.Icon(ft.icons.SETTINGS),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
            ],
        )

        self.log_submenu = ft.SubmenuButton(
            ref=self.log_submenu,
            content=ft.Text("Log"),
            controls=[
                ft.MenuItemButton(
                    content=ft.Text("View Log"),
                    leading=ft.Icon(ft.icons.DESCRIPTION),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
            ],
        )

        self.help_submenu = ft.SubmenuButton(
            ref=self.help_submenu,
            content=ft.Text("Help"),
            controls=[
                ft.MenuItemButton(
                    content=ft.Text("FAQ"),
                    leading=ft.Icon(ft.icons.HELP),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
                ft.MenuItemButton(
                    content=ft.Text("About"),
                    leading=ft.Icon(ft.icons.INFO),
                    style=ft.ButtonStyle(
                        bgcolor={ft.ControlState.HOVERED: ft.colors.GREEN_100}
                    ),
                    on_click=lambda e: self.controller.handle_menu_item_click(e),
                ),
            ],
        )

        self.menu_bar_view = ft.MenuBar(
            ref=self.menu_bar,
            expand=True,
            style=ft.MenuStyle(
                alignment=ft.alignment.top_left,
                bgcolor=ft.colors.GREY_200,
                mouse_cursor={
                    ft.ControlState.HOVERED: ft.MouseCursor.WAIT,
                    ft.ControlState.DEFAULT: ft.MouseCursor.ZOOM_OUT,
                },
            ),
            controls=[
                self.file_picker,
                self.template_loader,
                self.file_submenu,
                self.viewer_submenu,
                self.scheduler_submenu,
                self.upload_submenu,
                self.log_submenu,
                self.help_submenu,
            ],
        )

    def build_metadata_components(self):
        self.back_button = ft.ElevatedButton(
            ref=self.back_button,
            text="Go Back",
            on_click=lambda e: self.controller.on_back_click(e),
            icon=ft.icons.ARROW_BACK,
            tooltip="Go back to previous directory",
        )

        self.current_path = ft.Text(
            ref=self.current_path,
            value=self.controller.current_path,
            size=16,
            weight=ft.FontWeight.W_500,
            expand=True,
        )

        self.file_explorer = ft.ListView(
            ref=self.file_explorer,
            controls=[],
        )

        self.metadata_list = ft.ListView(
            ref=self.metadata_list,
            controls=[],
        )

    def build_selected_metadata_components(self):
        self.template_saver = ft.FilePicker(
            ref=self.template_saver, on_result=self.controller.on_save_as_template
        )

        self.selected_metadata = ft.ListView(
            ref=self.selected_metadata,
            spacing=10,
            padding=20,
            controls=[],
        )

        self.selected_meta_header = ft.Row(
            ref=self.selected_meta_header,
            controls=[
                ft.Text("", weight=ft.FontWeight.BOLD, width=60),
                ft.Text("path", weight=ft.FontWeight.BOLD, width=500),
                ft.Text("key", weight=ft.FontWeight.BOLD, width=200),
                ft.Text("value", weight=ft.FontWeight.BOLD, width=600),
            ],
        )

        self.selected_meta_content = ft.Row(
            ref=self.selected_meta_content,
            controls=[
                ft.Container(content=self.selected_metadata, height=200),
            ],
        )

        self.selected_meta_buttons = ft.Row(
            ref=self.selected_meta_buttons,
            controls=[
                ft.Container(
                    content=ft.Row(
                        controls=[
                            ft.ElevatedButton(
                                text="Export to Json",
                                on_click=lambda e: self.controller.on_export_to_json(e),
                            ),
                            ft.ElevatedButton(
                                text="Save as Template",
                                on_click=lambda e: self.template_saver.save_file(
                                    file_type="json",
                                    allowed_extensions=["json"],
                                ),
                            ),
                        ],
                        alignment=ft.MainAxisAlignment.END,
                    ),
                    expand=True,
                ),
            ],
        )

    def update_file_list_controls(self):
        """Display the file list"""
        controls = []
        for item in self.controller.file_list:
            full_path = item[0]
            is_dir = item[1] == DIRECTORY
            icon = ft.icons.FOLDER if is_dir else ft.icons.INSERT_DRIVE_FILE
            controls.append(
                ft.ListTile(
                    leading=ft.Icon(icon),
                    title=ft.Text(os.path.basename(full_path)),
                    on_click=lambda e, path=full_path: self.controller.on_file_click(
                        e, path
                    ),
                    trailing=(
                        ft.IconButton(
                            icon=ft.icons.ARROW_FORWARD,
                            on_click=lambda e, path=full_path: self.controller.on_directory_click(
                                e, path
                            ),
                            visible=is_dir,
                        )
                        if is_dir
                        else ft.Text(item[1])
                    ),
                )
            )
        self.file_explorer.controls = controls

    def show_jcampdx_metadata(self, path):
        self.metadata_list.controls = []
        for key, value in self.controller.meta_data.items():
            self.metadata_list.controls.append(
                ft.ListTile(
                    leading=ft.Checkbox(
                        value=(
                            True
                            if (path, key) in self.controller.checked_metadata
                            else False
                        ),
                        on_change=lambda e, path=path, key=key, value=value: self.controller.on_metadata_checked(
                            e, path, key, value
                        ),
                    ),
                    subtitle=ft.Row(
                        controls=[
                            ft.TextField(
                                value=key,
                                read_only=True,
                                border="none",
                                width=300,
                                text_size=12,
                            ),
                            ft.TextField(
                                value=value, read_only=True, width=600, text_size=12
                            ),
                        ]
                    ),
                )
            )

    def update_selected_metadata_controls(self):
        self.selected_metadata.controls = []
        for key, value in self.controller.checked_metadata.items():
            self.selected_metadata.controls.append(
                ft.Row(
                    controls=[
                        ft.IconButton(
                            icon=ft.icons.DELETE,
                            on_click=lambda e, path=key[0], key=key[
                                1
                            ]: self.controller.on_metadata_deleted(e, path, key),
                        ),
                        ft.Text(key[0], width=500),
                        ft.Text(key[1], width=200),
                        ft.Text(
                            value,
                            width=600,
                        ),
                    ]
                )
            )
