import contextlib
import os
import io
import sys
from io import StringIO
from pathlib import Path
from typing import Any, Generator

import pytest

EXAMPLE_SCRIPTS = sorted(Path(__file__).parent.glob("pyspark-examples/*.py"))


@pytest.fixture
def pyspark_examples_dir() -> Generator[None, Any, None]:
    prev = os.getcwd()
    os.chdir(Path(__file__).parent / "pyspark-examples")
    yield
    os.chdir(prev)


class CapturePrint:
    _output = []

    @property
    def output(self) -> str:
        return "".join(self._output)

    def __call__(self, *args, **kwargs):
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        self._output.append(sep.join(args) + end)


@contextlib.contextmanager
def capture_output() -> Generator[StringIO, Any, None]:
    prev_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield sys.stdout
    sys.stdout = prev_stdout


@pytest.mark.parametrize(
    "script_path",
    EXAMPLE_SCRIPTS[1:3],
    ids=[s.name for s in EXAMPLE_SCRIPTS[1:3]],
)
def test_pyspark_examples(
    script_path: Path,
    pyspark_examples_dir,
) -> None:
    """This test executes each script both with pyspark and pyspark-cl
    and verifies that the output is identical.
    """
    script = script_path.read_text()

    # Load, compile and run code, this is done so that the pyspark
    # session can be reused, and therefore testing is way faster
    pyspark_code = compile(script, script_path, "exec")
    pyspark_error = None
    with capture_output() as pyspark_output:
        try:
            exec(pyspark_code)
        except Exception as err:
            pyspark_error = err

    cl_code = compile(script.replace("pyspark", "pyspark_cl"), script_path, "exec")
    cl_error = None
    with capture_output() as cl_output:
        try:
            exec(cl_code)
        except Exception as err:
            cl_error = err

    assert pyspark_output.getvalue() == cl_output.getvalue()
    assert str(pyspark_error) == str(cl_error)
