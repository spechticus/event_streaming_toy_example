from dataclasses import dataclass, field, fields
from typing import List, Dict
import datetime
from os import makedirs
from os.path import join


@dataclass
class MockCloudWatch:
    ingested_events: int = 0
    duplicates_prevented: int = 0
    batch_duplicates: int = 0
    lambda_invocations: int = 0
    glue_invocations: int = 0
    staging_storage_used_in_mbytes: int = 0
    processed_storage_used_in_mbytes: int = 0
    total_storage_limit_in_mbytes: int = 2048
    ingestion_error_ratio: float = 0.0
    used_storage_percentage: float = 0.0

    def calculate_error_ratio(self) -> None:
        self.ingestion_error_ratio = round(
            self.duplicates_prevented / self.ingested_events, 3
        )

    def calculate_used_storage_percentage(self) -> None:
        self.used_storage_percentage = round(
            (
                (
                    self.staging_storage_used_in_mbytes
                    + self.processed_storage_used_in_mbytes
                )
                / self.total_storage_limit_in_mbytes
            ),
            2,
        )

    def to_markdown(self, markdown_file_path) -> None:
        # Ensure the directory exists
        makedirs(markdown_file_path, exist_ok=True)

        # Start composing the markdown content
        markdown_content = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - CloudWatch Metrics Report\n"

        for field_info in fields(self):
            value = getattr(self, field_info.name)
            markdown_content += f"## {field_info.name.replace('_', ' ').title()}\n"
            markdown_content += f"Value: {value:}\n\n"

        # Write the entire markdown content to the file after the loop
        full_path = join(markdown_file_path, "cloudwatch_report.md")
        print(f"Writing to {full_path}")
        with open(full_path, "a") as md_file:
            md_file.write(markdown_content)
