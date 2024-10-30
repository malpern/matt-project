import logging
from typing import List, Dict
from datetime import datetime
import pygsheets
from collections import defaultdict
from enum import Enum

class ChartType(Enum):
    """Enum for the type of chart."""
    BAR = 'BAR'
    LINE = 'LINE'
    AREA = 'AREA'
    COLUMN = 'COLUMN'
    SCATTER = 'SCATTER'
    COMBO = 'COMBO'

class ChartTypeWrapper:
    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

class RevenueSummary:
    def __init__(self, sheet_manager):
        self.sheet_manager = sheet_manager
        self.logger = logging.getLogger(__name__)

    def create_revenue_summary(self):
        """Creates or updates the REVENUE SUMMARY tab with all charts."""
        try:
            self.logger.info("Creating REVENUE SUMMARY tab...")
            revenue_summary_sheet = self.sheet_manager.clear_or_create_tab("REVENUE SUMMARY")

            # Fetch data from the Sales & Sessions Completed sheets
            current_year = datetime.now().year
            current_year_data = self.get_sales_data(current_year)
            last_year_data = self.get_sales_data(current_year - 1)

            # Create charts (handle empty last_year_data gracefully)
            if not current_year_data and not last_year_data:
                self.logger.warning("No data available for revenue summary")
                return

            # Create each chart in a try-except block
            for chart_method in [
                self.create_revenue_chart,
                self.create_sessions_chart,
                self.create_churn_rate_chart,
                self.create_new_clients_chart,
                self.create_returning_clients_chart
            ]:
                try:
                    chart_method(revenue_summary_sheet, current_year_data, last_year_data)
                except Exception as e:
                    self.logger.error(f"Failed to create {chart_method.__name__}: {str(e)}")
                    self.logger.error(f"Traceback: ", exc_info=True)

            self.logger.info("REVENUE SUMMARY tab created successfully.")
        except Exception as e:
            self.logger.error(f"Failed to create revenue summary: {str(e)}")
            raise

    def get_sales_data(self, year: int) -> List[Dict]:
        """Fetches and processes sales data for a given year."""
        try:
            # Get the current year's sheet since all data is there
            sheet = self.sheet_manager.find_sales_sheet(datetime.now().year)
            if not sheet:
                self.logger.warning(f"Sales sheet not found, returning empty data")
                return []
                
            data = sheet.get_all_values()
            headers = data[0]
            date_col = headers.index("DATE")
            client_col = headers.index("CLIENT NAME")
            price_col = headers.index("PRICE PER SESSION")

            processed_data = []
            for row in data[1:]:  # Skip header row
                try:
                    # Parse date and check year
                    date = datetime.strptime(row[date_col], "%m/%d/%Y")
                    if date.year != year:  # Skip rows from other years
                        continue
                    
                    # Handle various price formats
                    price_str = row[price_col].replace("$", "").strip()
                    if price_str in ["???", "", "DUE???", "MONTHLY CALC??"]:
                        price = 0  # Default to 0 for unknown prices
                    else:
                        price = float(price_str)
                    
                    processed_data.append({
                        "date": date,
                        "client": row[client_col],
                        "price": price
                    })
                except (ValueError, IndexError) as e:
                    self.logger.warning(f"Error processing row: {row}. Error: {str(e)}")
                    continue
                
            self.logger.info(f"Processed {len(processed_data)} rows for year {year}")
            return processed_data
        except Exception as e:
            self.logger.error(f"Error processing sales data for year {year}: {str(e)}")
            return []

    def aggregate_monthly_data(self, data: List[Dict], field: str) -> Dict[int, float]:
        """Aggregates data by month for a given field."""
        monthly_data = defaultdict(float)
        for entry in data:
            month = entry['date'].month
            if field == 'count':
                monthly_data[month] += 1
            else:
                monthly_data[month] += entry[field]
        return monthly_data

    def calculate_churn_rate(self, data: List[Dict]) -> Dict[int, float]:
        """Calculates monthly churn rate."""
        monthly_clients = defaultdict(set)
        for entry in data:
            month = entry["date"].month
            monthly_clients[month].add(entry["client"])

        churn_rate = {}
        for month in range(2, 13):  # Start from February
            prev_clients = monthly_clients[month - 1]
            current_clients = monthly_clients[month]
            if prev_clients:
                churned = len(prev_clients - current_clients)
                churn_rate[month] = (churned / len(prev_clients)) * 100
            else:
                churn_rate[month] = 0
        return churn_rate

    def calculate_new_clients(self, data: List[Dict]) -> Dict[int, int]:
        """Calculates new clients per month."""
        seen_clients = set()
        monthly_new_clients = defaultdict(int)
        
        for entry in sorted(data, key=lambda x: x["date"]):
            month = entry["date"].month
            if entry["client"] not in seen_clients:
                monthly_new_clients[month] += 1
                seen_clients.add(entry["client"])
        
        return monthly_new_clients

    def calculate_returning_clients(self, data: List[Dict]) -> Dict[int, int]:
        """Calculates returning clients per month."""
        monthly_clients = defaultdict(set)
        for entry in data:
            month = entry["date"].month
            monthly_clients[month].add(entry["client"])
        
        returning_clients = {}
        all_clients = set()
        for month in range(1, 13):
            current_clients = monthly_clients[month]
            returning_clients[month] = len(current_clients & all_clients)
            all_clients.update(current_clients)
        
        return returning_clients

    def create_revenue_chart(self, sheet, current_year_data, last_year_data):
        """Creates a revenue by month chart comparing current and last year."""
        current_revenue = self.aggregate_monthly_data(current_year_data, 'price')
        last_revenue = self.aggregate_monthly_data(last_year_data, 'price')
        
        # Prepare data for chart
        headers = ['Month', str(datetime.now().year - 1), str(datetime.now().year)]
        data = [headers]
        for month in range(1, 13):
            row = [
                datetime(2000, month, 1).strftime('%B'),
                last_revenue.get(month, 0),
                current_revenue.get(month, 0)
            ]
            data.append(row)
        
        sheet.update_values('A1', data)
        
        try:
            chart_type = ChartTypeWrapper('COLUMN')
            chart = sheet.add_chart(
                ('A1', f'A{len(data)}'),
                [('B1', f'B{len(data)}'), ('C1', f'C{len(data)}')],
                chart_type,  # Use our wrapper object
                'Monthly Revenue Comparison',
                'E1'
            )
            chart.set_legend_position('RIGHT')
            self.logger.info("Successfully created revenue chart")
        except Exception as e:
            self.logger.error(f"Failed to create revenue chart: {str(e)}", exc_info=True)

    def create_sessions_chart(self, sheet, current_year_data, last_year_data):
        """Creates a sessions by month chart comparing current and last year."""
        current_sessions = self.aggregate_monthly_data(current_year_data, 'count')
        last_sessions = self.aggregate_monthly_data(last_year_data, 'count')
        
        start_row = 15
        headers = ['Month', str(datetime.now().year - 1), str(datetime.now().year)]
        data = [headers]
        for month in range(1, 13):
            row = [
                datetime(2000, month, 1).strftime('%B'),
                last_sessions.get(month, 0),
                current_sessions.get(month, 0)
            ]
            data.append(row)
        
        # Update sheet with data
        sheet.update_values(f'A{start_row}', data)
        
        try:
            chart = sheet.add_chart(
                (f'A{start_row}', f'A{start_row + len(data) - 1}'),  # domain
                [(f'B{start_row}', f'B{start_row + len(data) - 1}'), 
                 (f'C{start_row}', f'C{start_row + len(data) - 1}')],  # ranges
                'COLUMN',  # Just use the string directly
                'Monthly Sessions Comparison',
                f'E{start_row}'
            )
            chart.set_legend_position('RIGHT')
            self.logger.info("Successfully created sessions chart")
        except Exception as e:
            self.logger.error(f"Failed to create sessions chart: {str(e)}", exc_info=True)

    def create_churn_rate_chart(self, sheet, current_year_data, last_year_data):
        """Creates a churn rate chart comparing current and last year."""
        current_churn = self.calculate_churn_rate(current_year_data)
        last_churn = self.calculate_churn_rate(last_year_data)
        
        start_row = 30
        headers = ['Month', str(datetime.now().year - 1), str(datetime.now().year)]
        data = [headers]
        for month in range(1, 13):
            row = [
                datetime(2000, month, 1).strftime('%B'),
                last_churn.get(month, 0),
                current_churn.get(month, 0)
            ]
            data.append(row)
        
        sheet.update_values(f'A{start_row}', data)
        
        try:
            chart = sheet.add_chart(
                (f'A{start_row}', f'A{start_row + len(data) - 1}'),  # domain
                [(f'B{start_row}', f'B{start_row + len(data) - 1}'), 
                 (f'C{start_row}', f'C{start_row + len(data) - 1}')],  # ranges
                'COLUMN',  # Just use the string directly
                'Monthly Churn Rate Comparison',
                f'E{start_row}'
            )
            chart.set_legend_position('RIGHT')
            self.logger.info("Successfully created churn rate chart")
        except Exception as e:
            self.logger.error(f"Failed to create churn rate chart: {str(e)}", exc_info=True)

    def create_new_clients_chart(self, sheet, current_year_data, last_year_data):
        """Creates a new clients chart comparing current and last year."""
        current_new = self.calculate_new_clients(current_year_data)
        last_new = self.calculate_new_clients(last_year_data)
        
        start_row = 45
        headers = ['Month', str(datetime.now().year - 1), str(datetime.now().year)]
        data = [headers]
        for month in range(1, 13):
            row = [
                datetime(2000, month, 1).strftime('%B'),
                last_new.get(month, 0),
                current_new.get(month, 0)
            ]
            data.append(row)
        
        sheet.update_values(f'A{start_row}', data)
        try:
            chart = sheet.add_chart(
                (f'A{start_row}', f'A{start_row + len(data) - 1}'),
                [(f'B{start_row}', f'B{start_row + len(data) - 1}'), 
                 (f'C{start_row}', f'C{start_row + len(data) - 1}')],
                'COLUMN',  # Just use the string directly
                'Monthly New Clients Comparison',
                f'E{start_row}'
            )
            chart.set_legend_position('RIGHT')
            self.logger.info("Successfully created new clients chart")
        except Exception as e:
            self.logger.error(f"Failed to create new clients chart: {str(e)}", exc_info=True)

    def create_returning_clients_chart(self, sheet, current_year_data, last_year_data):
        """Creates a returning clients chart comparing current and last year."""
        current_returning = self.calculate_returning_clients(current_year_data)
        last_returning = self.calculate_returning_clients(last_year_data)
        
        start_row = 60
        headers = ['Month', str(datetime.now().year - 1), str(datetime.now().year)]
        data = [headers]
        for month in range(1, 13):
            row = [
                datetime(2000, month, 1).strftime('%B'),
                last_returning.get(month, 0),
                current_returning.get(month, 0)
            ]
            data.append(row)
        
        sheet.update_values(f'A{start_row}', data)
        try:
            chart = sheet.add_chart(
                (f'A{start_row}', f'A{start_row + len(data) - 1}'),
                [(f'B{start_row}', f'B{start_row + len(data) - 1}'), 
                 (f'C{start_row}', f'C{start_row + len(data) - 1}')],
                'COLUMN',  # Just use the string directly
                'Monthly Returning Clients Comparison',
                f'E{start_row}'
            )
            chart.set_legend_position('RIGHT')
            self.logger.info("Successfully created returning clients chart")
        except Exception as e:
            self.logger.error(f"Failed to create returning clients chart: {str(e)}", exc_info=True)
