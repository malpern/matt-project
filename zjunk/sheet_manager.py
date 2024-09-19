import logging
import pygsheets

class SheetManager:
    def __init__(self, gc: pygsheets.client.Client, spreadsheet_name: str):
        self.gc = gc
        self.spreadsheet = self.gc.open(spreadsheet_name)

    def clear_or_create_tab(self, tab_name: str) -> pygsheets.Worksheet:
        try:
            sheet = self.spreadsheet.worksheet_by_title(tab_name)
            logging.info(f"Clearing existing '{tab_name}' tab...")
            sheet.clear()
        except pygsheets.exceptions.WorksheetNotFound:
            logging.info(f"Creating '{tab_name}' tab...")
            sheet = self.spreadsheet.add_worksheet(tab_name)
        return sheet

    def get_sheet(self, tab_name: str) -> pygsheets.Worksheet:
        return self.spreadsheet.worksheet_by_title(tab_name)

    def find_sales_sheet(self, year: int) -> pygsheets.Worksheet:
        sales_tab_name = f"Sales & Sessions Completed {year}"
        logging.info(f"Searching for '{sales_tab_name}' tab...")
        for worksheet in self.spreadsheet.worksheets():
            if sales_tab_name in worksheet.title:
                return worksheet
        raise ValueError(f"Could not find the '{sales_tab_name}' tab.")