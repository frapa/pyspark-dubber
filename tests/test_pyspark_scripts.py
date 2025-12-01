import os
import traceback
from pathlib import Path
from typing import Any, Generator

import ibis
import pytest

from tests.conftest import capture_output

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
    ), f"See original error above for more details. Stdout:\n{dubber_stdout}"
    assert dubber_stdout == pyspark_stdout

    # So you can check the output for reference
    print(dubber_stdout)

    # Check output files and content are identical
    pyspark_files = sorted(
        p.relative_to(pyspark_dir)
        for p in pyspark_dir.glob("**/*")
        if not p.is_dir() and p.parts[1] != "data" and p.suffix not in {".crc", ""}
    )
    pyspark_dirs = {p.parent for p in pyspark_files}
    dubber_files = sorted(
        p.relative_to(dubber_dir)
        for p in dubber_dir.glob("**/*")
        if not p.is_dir() and p.parts[1] != "data"
    )
    dubber_dirs = {p.parent for p in dubber_files}
    # Spark writes files in a very different way,
    # with each partition as a separate file and checksum
    # and _SUCCESS files. We don't aim to reproduce
    # this right now, so we only check folder names.
    assert dubber_dirs == pyspark_dirs

    for rel_path in pyspark_dirs:
        pyspark_files = [pyspark_dir / f for f in pyspark_files if f.parent == rel_path]
        dubber_files = [dubber_dir for f in dubber_files if f.parent == rel_path]

        ext = {f.suffix for f in pyspark_files}
        assert len(ext), ext
        if ext == {".csv"}:
            load_func = (
                lambda ps: ibis.read_csv(ps).to_pandas().to_dict(orient="records")
            )
        else:
            raise NotImplementedError(f"Unsupported file type: {ext}")

        pyspark_data = load_func(pyspark_files)
        dubber_data = load_func(dubber_files)
        assert dubber_data == pyspark_data
