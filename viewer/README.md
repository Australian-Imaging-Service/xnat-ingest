## Release Notes

### Version 0.1 (2024-12-02)
- added a simple MVC framework in viewer/core
- added menubar in home view
- added function to open local folder
- added function to navigate through folders
- added function to show current path under the menubar
- added a back button to go back to the previous folder
- added a button to navigate into a specific folder
- added the right panel to show the metadata of the selected file
- added checkedbox to each metadata entry
- added function to check/uncheck metadata entries
- added a bottom panel to show the selected metadata entries
- added function to remove selected metadata entries from the bottom panel
- added support to sync the check/uncheck status of the metadata entries with the bottom panel
- added a button to export the selected metadata entries as a template and save as a json file
- added function to load a template file and check/uncheck the metadata entries accordingly

## TODO
- function to export the selected metadata entries as a json file
- handle list datatype in metadata entries
- handle nested metadata entries
- add function to select DICOM files in the PVdataset for uploading
- link the GUI with the xnat-ingest API for uploading to XNAT
- add function in the scheduler to schdule the uploading tasks
- add function in the scheduler to monitor the uploading tasks
- add function to show the logs in the GUI