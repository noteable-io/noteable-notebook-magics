import abc
from typing import Any, Iterable

from click import Command, Option
from pydantic import BaseModel
from rich import print as rprint


class OutputModel(abc.ABC, BaseModel):
    @abc.abstractmethod
    def get_human_readable_output(self) -> Iterable[Any]:
        pass

    def get_js_readable_output(self, **kwargs) -> str:
        """The output of this method should be parseable by javascript, generally JSON"""
        return self.model_dump_json(**kwargs)


class NTBLCommand(Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.append(Option(["--json"], is_flag=True, help="Show output as JSON"))
        self.params.append(Option(["--json-pretty"], is_flag=True, help="Pretty print JSON output"))

    def invoke(self, ctx):
        return_json = ctx.params.pop("json")
        json_pretty = ctx.params.pop("json_pretty")
        result = super().invoke(ctx)

        if isinstance(result, OutputModel):
            if json_pretty:
                rprint(result.get_js_readable_output(indent=4))
            elif return_json:
                print(result.get_js_readable_output())
            else:
                for output in result.get_human_readable_output():
                    rprint(output)
            return None

        return result
