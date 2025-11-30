import contextlib
import os
import io
import sys
import traceback
from io import StringIO
from pathlib import Path
from typing import Any, Generator

import pytest

DATA_DIR = Path(__file__).parent / "data"

SCRIPTS = sorted(Path(__file__).parent.glob("scripts/*.py"))
EXAMPLE_SCRIPTS = sorted(Path(__file__).parent.glob("pyspark-examples/*.py"))
TEST_CASES = [
    *SCRIPTS[:4],
    # examples from the internet, mostly bad quality
    *EXAMPLE_SCRIPTS[1:3],
]


@pytest.fixture
def test_dir(tmpdir: Path) -> Generator[Path, Any, None]:
    (tmpdir / "pyspark").mkdir()
    os.symlink(DATA_DIR, tmpdir / "pyspark" / "data")
    (tmpdir / "dubber").mkdir()
    os.symlink(DATA_DIR, tmpdir / "dubber" / "data")
    yield Path(tmpdir)


@contextlib.contextmanager
def capture_output() -> Generator[StringIO, Any, None]:
    prev_stdout = sys.stdout
    sys.stdout = io.StringIO()
    yield sys.stdout
    sys.stdout = prev_stdout


@pytest.mark.parametrize(
    "script_path",
    TEST_CASES,
    ids=[s.name for s in TEST_CASES],
)
def test_scripts(
    script_path: Path,
    test_dir: Path,
) -> None:
    """This test executes each script both with pyspark and pyspark-dubber
    and verifies that the output is identical.
    """
    script = script_path.read_text()

    # Load, compile and run code, this is done so that the pyspark
    # session can be reused, and therefore testing is way faster
    pyspark_code = compile(script, script_path, "exec")
    pyspark_error = None
    pyspark_dir = test_dir / "pyspark"
    os.chdir(pyspark_dir)
    with capture_output() as pyspark_output:
        try:
            exec(pyspark_code, globals())
        except Exception as err:
            traceback.print_exc()
            pyspark_error = err

    dubber_code = compile(
        script.replace("pyspark", "pyspark_dubber"), script_path, "exec"
    )
    dubber_err = None
    dubber_dir = test_dir / "dubber"
    os.chdir(dubber_dir)
    with capture_output() as dubber_output:
        try:
            exec(dubber_code, globals())
        except Exception as err:
            traceback.print_exc()
            dubber_err = err

    dubber_stdout = dubber_output.getvalue()
    pyspark_stdout = pyspark_output.getvalue()

    # For certain tests we might need an override for very niche incompatibilities
    if script_path.name == "pandas-pyspark-dataframe.py":
        # pyspark uses an intermediate class for pandas conversion
        # that we don't want to implement (the example is just poorly written)
        pyspark_stdout = pyspark_stdout.replace("PandasConversionMixin", "DataFrame")

    assert str(dubber_err) == str(
        pyspark_error
    ), f"See original error above for more details. Stdout:\n{dubber_output.getvalue()}"
    assert dubber_stdout == pyspark_stdout

    # So you can check the output for reference
    print(dubber_stdout)

    # Check output files and content are identical
    pyspark_files = sorted(
        p.relative_to(pyspark_dir)
        for p in pyspark_dir.glob("**/*")
        if not p.is_dir() and p.parts[1] != "data"
    )
    dubber_files = sorted(
        p.relative_to(dubber_dir)
        for p in dubber_dir.glob("**/*")
        if not p.is_dir() and p.parts[1] != "data"
    )
    assert dubber_files == pyspark_files

    for rel_path in pyspark_files:
        pyspark_path = pyspark_dir / rel_path
        dubber_path = dubber_dir / rel_path
        print(
            rel_path,
            f"pyspark: {pyspark_path.stat().st_size} bytes",
            f"dubber: {dubber_path.stat().st_size} bytes",
        )
