from __future__ import annotations

from typing import Optional
from airflow.utils.context import Context
from airflow.providers.docker.operators.docker import DockerOperator


class DockerOperatorWithDisplay(DockerOperator):
    template_fields = (*DockerOperator.template_fields, "display_name")

    def __init__(self, *, display_name: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.display_name = display_name

    def get_task_display_name(self, context: Context) -> str:
        if self.display_name:
            return self.display_name
        return super().get_task_display_name(context)
