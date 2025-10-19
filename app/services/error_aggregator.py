import csv
import io

class ErrorAggregator:
    def __init__(self):
        self.failed_rows = []
        self.success_count = 0
        self.total_rows_processed = 0

    def add_failure(self, row_number, row_data, error_message):
        self.failed_rows.append({
            'row_number': row_number,
            'row_data': row_data,
            'error_message': error_message
        })
        self.total_rows_processed += 1

    def add_success(self):
        self.success_count += 1
        self.total_rows_processed += 1

    @property
    def failure_count(self):
        return len(self.failed_rows)

    @property
    def has_failures(self):
        return self.failure_count > 0

    def get_summary_dict(self):
        status = "Completed with errors" if self.has_failures else "Completed successfully"
        return {
            'status': status,
            'total_rows_processed': self.total_rows_processed,
            'success_count': self.success_count,
            'failure_count': self.failure_count
        }

    def generate_error_csv_string(self):
        if not self.has_failures:
            return None

        output = io.StringIO()

        # Get headers from the first failed row's data and add the new 'Error Message' header
        if self.failed_rows:
            headers = list(self.failed_rows[0]['row_data'].keys()) + ['Error Message']
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()

            # Write failed rows with their corresponding error messages
            for failure in self.failed_rows:
                row_to_write = failure['row_data'].copy()
                row_to_write['Error Message'] = failure['error_message']
                writer.writerow(row_to_write)

        return output.getvalue()
