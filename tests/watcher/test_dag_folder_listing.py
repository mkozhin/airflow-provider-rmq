"""The walk of the DAG folder by Airflow's rules: ``list_dag_files`` and ``is_listed``.

Every test builds a real directory tree under ``tmp_path``. Tests that take access
away with ``chmod`` are skipped when running as root, where permission bits do not
deny anything.
"""

from __future__ import annotations

import builtins
import errno
import locale
import os
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_provider_rmq.watcher.dag_folder_listing import (
    DagFolderListing,
    DagFolderListingError,
    is_listed,
    list_dag_files,
)

SYNTAX_ENV = "AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX"

needs_permissions = pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root is not denied access by permission bits",
)


@pytest.fixture
def folder(tmp_path):
    path = tmp_path / "dags"
    path.mkdir()
    return path


@pytest.fixture(autouse=True)
def default_syntax(monkeypatch):
    """Every test starts from the default ``dag_ignore_file_syntax``."""
    monkeypatch.delenv(SYNTAX_ENV, raising=False)


@pytest.fixture
def chmod():
    """``chmod`` that gives the path its access back after the test, for cleanup."""
    changed: list[Path] = []

    def _chmod(path: Path, mode: int) -> None:
        changed.append(path)
        os.chmod(path, mode)

    yield _chmod
    for path in reversed(changed):
        os.chmod(path, 0o755)


def _touch(base: Path, rel: str, content: str = "") -> str:
    path = base / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path)


def _set_syntax(monkeypatch, syntax: str) -> None:
    monkeypatch.setenv(SYNTAX_ENV, syntax)


def _files(base: Path, *rels: str) -> set[str]:
    return {str(base / rel) for rel in rels}


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


class TestListing:
    def test_flat_folder(self, folder):
        _touch(folder, "a.py")
        _touch(folder, "b.py")
        _touch(folder, "notes.txt")

        listing = list_dag_files(str(folder))

        assert listing == DagFolderListing(_files(folder, "a.py", "b.py"), {}, set())

    def test_nested_directories(self, folder):
        _touch(folder, "a.py")
        _touch(folder, "sub/b.py")
        _touch(folder, "sub/deep/c.py")

        assert list_dag_files(str(folder)).files == _files(
            folder, "a.py", "sub/b.py", "sub/deep/c.py"
        )

    def test_hidden_file_and_hidden_directory_are_listed(self, folder):
        _touch(folder, ".x.py")
        _touch(folder, ".hidden/a.py")

        assert list_dag_files(str(folder)).files == _files(folder, ".x.py", ".hidden/a.py")

    def test_regexp_ignore_file(self, folder):
        _touch(folder, ".airflowignore", "ignored_dir\nskip_.*\\.py  # comment\n")
        _touch(folder, "a.py")
        _touch(folder, "skip_me.py")
        _touch(folder, "ignored_dir/b.py")

        assert list_dag_files(str(folder)).files == _files(folder, "a.py")

    def test_glob_ignore_file_with_negation(self, folder, monkeypatch):
        _set_syntax(monkeypatch, "glob")
        _touch(folder, ".airflowignore", "*.tmp.py\n!keep.tmp.py\nignored_dir/\n")
        _touch(folder, "a.py")
        _touch(folder, "drop.tmp.py")
        _touch(folder, "keep.tmp.py")
        _touch(folder, "ignored_dir/b.py")

        assert list_dag_files(str(folder)).files == _files(folder, "a.py", "keep.tmp.py")

    def test_nested_ignore_file_inherits_the_parent_rules(self, folder):
        _touch(folder, ".airflowignore", "foo\n")
        _touch(folder, "sub/.airflowignore", "bar\n")
        _touch(folder, "bar.py")
        _touch(folder, "sub/foo.py")
        _touch(folder, "sub/bar.py")
        _touch(folder, "sub/ok.py")

        assert list_dag_files(str(folder)).files == _files(folder, "bar.py", "sub/ok.py")

    def test_symlinked_directory_is_walked(self, folder, tmp_path):
        _touch(tmp_path, "outside/z.py")
        (folder / "link").symlink_to(tmp_path / "outside")

        assert list_dag_files(str(folder)).files == _files(folder, "link/z.py")

    def test_entries_that_are_not_py_files_are_not_listed(self, folder):
        _touch(folder, ".py")
        (folder / "dead.py").symlink_to(folder / "missing.py")
        with zipfile.ZipFile(folder / "bundle.zip", "w") as archive:
            archive.writestr("in_zip.py", "")
        (folder / "pkg.py").mkdir()
        _touch(folder, "a.py")

        listing = list_dag_files(str(folder))

        assert listing.files == _files(folder, "a.py")
        assert listing.unreadable_dirs == {}

    def test_folder_with_a_trailing_slash(self, folder):
        _touch(folder, "a.py")
        _touch(folder, "sub/b.py")

        listing = list_dag_files(str(folder) + "/")

        assert listing.files == {
            os.path.join(str(folder) + "/", "a.py"),
            os.path.join(str(folder) + "/", "sub", "b.py"),
        }
        assert listing.files == _files(folder, "a.py", "sub/b.py")


# ---------------------------------------------------------------------------
# I/O errors
# ---------------------------------------------------------------------------


@needs_permissions
class TestUnreadable:
    @pytest.mark.parametrize("mode", [0o000, 0o644, 0o311], ids=["000", "644", "311"])
    def test_unreadable_directory(self, folder, chmod, mode):
        _touch(folder, "a.py")
        _touch(folder, "locked/b.py")
        _touch(folder, "locked/deeper/c.py")
        _touch(folder, "open/d.py")
        chmod(folder / "locked", mode)

        listing = list_dag_files(str(folder))

        assert listing.files == _files(folder, "a.py", "open/d.py")
        assert set(listing.unreadable_dirs) == {str(folder / "locked")}
        assert isinstance(listing.unreadable_dirs[str(folder / "locked")], PermissionError)

    def test_unreadable_ignore_file(self, folder, chmod):
        _touch(folder, "a.py")
        _touch(folder, "sub/.airflowignore", "x\n")
        _touch(folder, "sub/b.py")
        _touch(folder, "sub/deeper/c.py")
        chmod(folder / "sub/.airflowignore", 0o000)

        listing = list_dag_files(str(folder))

        assert listing.files == _files(folder, "a.py")
        assert set(listing.unreadable_dirs) == {str(folder / "sub")}

    def test_unreadable_root_ignore_file(self, folder, chmod):
        _touch(folder, ".airflowignore", "x\n")
        _touch(folder, "a.py")
        chmod(folder / ".airflowignore", 0o000)

        listing = list_dag_files(str(folder))

        assert listing.files == set()
        assert set(listing.unreadable_dirs) == {str(folder)}

    def test_symlinked_directory_whose_target_cannot_be_reached(self, folder, tmp_path, chmod):
        _touch(tmp_path, "outside/parent/target/q.py")
        (folder / "link").symlink_to(tmp_path / "outside/parent/target")
        _touch(folder, "a.py")
        chmod(tmp_path / "outside/parent", 0o600)

        listing = list_dag_files(str(folder))

        assert listing.files == _files(folder, "a.py")
        assert set(listing.unreadable_dirs) == {str(folder / "link")}
        assert isinstance(listing.unreadable_dirs[str(folder / "link")], PermissionError)

    def test_glob_directory_pattern_on_a_symlinked_directory_that_cannot_be_reached(
        self, folder, tmp_path, chmod, monkeypatch
    ):
        _set_syntax(monkeypatch, "glob")
        _touch(folder, ".airflowignore", "cache/\n")
        _touch(tmp_path, "outside/parent/target/q.py")
        (folder / "link").symlink_to(tmp_path / "outside/parent/target")
        _touch(folder, "a.py")
        chmod(tmp_path / "outside/parent", 0o600)

        listing = list_dag_files(str(folder))

        assert listing.files == _files(folder, "a.py")
        assert set(listing.unreadable_dirs) == {str(folder / "link")}

    @pytest.mark.parametrize("where", ["", "sub/"], ids=["root", "subdirectory"])
    def test_py_symlink_into_a_locked_directory(self, folder, tmp_path, chmod, where):
        _touch(tmp_path, "outside/locked/real.py")
        _touch(folder, f"{where}normal.py")
        _touch(folder, "other.py")
        (folder / f"{where}linked.py").symlink_to(tmp_path / "outside/locked/real.py")
        chmod(tmp_path / "outside/locked", 0o000)

        listing = list_dag_files(str(folder))

        assert listing.files == _files(
            folder, f"{where}normal.py", f"{where}linked.py", "other.py"
        )
        assert listing.unreadable_dirs == {}

    def test_dangling_symlink_without_py_suffix(self, folder):
        (folder / "dangling").symlink_to(folder / "missing")
        _touch(folder, "a.py")

        assert list_dag_files(str(folder)) == DagFolderListing(_files(folder, "a.py"), {}, set())

    def test_recheck_reports_a_directory_read_again(self, folder):
        _touch(folder, "sub/a.py")

        listing = list_dag_files(str(folder), recheck={str(folder / "sub")})

        assert listing.recovered == {str(folder / "sub")}
        assert listing.files == _files(folder, "sub/a.py")

    def test_recheck_does_not_report_a_directory_under_an_unreadable_parent(self, folder, chmod):
        _touch(folder, "parent/child/a.py")
        chmod(folder / "parent", 0o000)

        listing = list_dag_files(str(folder), recheck={str(folder / "parent/child")})

        assert listing.recovered == set()
        assert set(listing.unreadable_dirs) == {str(folder / "parent")}

    def test_recheck_does_not_report_a_directory_still_unreadable(self, folder, chmod):
        _touch(folder, "sub/a.py")
        chmod(folder / "sub", 0o000)

        listing = list_dag_files(str(folder), recheck={str(folder / "sub")})

        assert listing.recovered == set()
        assert set(listing.unreadable_dirs) == {str(folder / "sub")}


class TestListingErrors:
    def test_symlink_loop(self, folder):
        _touch(folder, "a.py")
        (folder / "loop").symlink_to(folder)

        with pytest.raises(DagFolderListingError, match="symlink loop"):
            list_dag_files(str(folder))

    def test_unsupported_syntax(self, folder, monkeypatch):
        _set_syntax(monkeypatch, "Glob")
        _touch(folder, "a.py")

        with pytest.raises(DagFolderListingError, match="'Glob' is not supported"):
            list_dag_files(str(folder))

    def test_invalid_glob_pattern(self, folder, monkeypatch):
        _set_syntax(monkeypatch, "glob")
        _touch(folder, ".airflowignore", "!\n")
        _touch(folder, "a.py")

        with pytest.raises(DagFolderListingError, match=".airflowignore cannot be applied"):
            list_dag_files(str(folder))

    @pytest.mark.skipif(
        locale.getpreferredencoding(False).lower().replace("-", "") != "utf8",
        reason="the ignore file is read in the locale encoding, which must be UTF-8 here",
    )
    def test_ignore_file_that_does_not_decode(self, folder):
        (folder / ".airflowignore").write_bytes(b"\xff\xfe\xfa\n")
        _touch(folder, "a.py")

        with pytest.raises(DagFolderListingError, match="UnicodeDecodeError"):
            list_dag_files(str(folder))

    def test_name_not_in_utf8_under_regexp_rules(self, folder):
        _touch(folder, ".airflowignore", "nothing_matches\n")
        fd = os.open(os.fsencode(str(folder)) + b"/\xff.py", os.O_CREAT | os.O_WRONLY)
        os.close(fd)

        with pytest.raises(DagFolderListingError, match="UnicodeEncodeError"):
            list_dag_files(str(folder))


# ---------------------------------------------------------------------------
# Invariant: an OSError of the walk never leaves list_dag_files
# ---------------------------------------------------------------------------


def _invariant_tree(folder: Path, outside: Path, syntax: str) -> list[str]:
    """A tree with every kind of entry; returns every path in it."""
    directory_pattern = "zzz/" if syntax == "glob" else "zzz"
    _touch(folder, ".airflowignore", f"{directory_pattern}\n")
    _touch(folder, "a.py")
    _touch(folder, "notes.txt")
    _touch(folder, "sub/b.py")
    _touch(folder, "sub/.airflowignore", f"{directory_pattern}\n")
    _touch(outside, "target/z.py")
    _touch(outside, "file.txt")
    (folder / "link").symlink_to(outside / "target")
    (folder / "flink").symlink_to(outside / "file.txt")
    return [
        str(folder),
        *(str(folder / rel) for rel in (
            ".airflowignore", "a.py", "notes.txt", "sub", "sub/b.py", "sub/.airflowignore",
            "link", "link/z.py", "flink",
        )),
    ]


def _raising(original, target: str):
    """Wrap ``original`` so that it raises ``OSError(EIO)`` on ``target`` only."""

    def fake(first, *args, **kwargs):
        if isinstance(first, (str, os.PathLike)) and os.fspath(first) == target:
            raise OSError(errno.EIO, "injected", target)
        return original(first, *args, **kwargs)

    return fake


def _patchers(target: str):
    """One patch per file-system call of the walk, raising on ``target``."""
    return {
        "Path.is_file": patch.object(Path, "is_file", _raising(Path.is_file, target)),
        "Path.is_dir": patch.object(Path, "is_dir", _raising(Path.is_dir, target)),
        "os.stat": patch.object(os, "stat", _raising(os.stat, target)),
        "os.lstat": patch.object(os, "lstat", _raising(os.lstat, target)),
        "open": patch.object(builtins, "open", _raising(builtins.open, target)),
    }


class TestInvariant:
    @pytest.mark.parametrize("syntax", ["regexp", "glob"])
    def test_only_the_listing_error_leaves_the_walk(self, tmp_path, monkeypatch, syntax):
        _set_syntax(monkeypatch, syntax)
        folder = tmp_path / "dags"
        folder.mkdir()
        targets = _invariant_tree(folder, tmp_path / "outside", syntax)

        for target in targets:
            for name, patcher in _patchers(target).items():
                with patcher:
                    try:
                        list_dag_files(str(folder))
                    except DagFolderListingError:
                        pass
                    except Exception as exc:
                        raise AssertionError(f"{name} raising on {target} leaks {exc!r}") from exc

    @pytest.mark.parametrize("rel", ["a.py", "sub/b.py"])
    def test_is_file_error_on_a_py_file_keeps_it_listed(self, tmp_path, rel):
        folder = tmp_path / "dags"
        folder.mkdir()
        _invariant_tree(folder, tmp_path / "outside", "regexp")
        target = str(folder / rel)

        with _patchers(target)["Path.is_file"]:
            listing = list_dag_files(str(folder))

        assert target in listing.files
        assert listing.unreadable_dirs == {}

    @pytest.mark.parametrize("rel", ["sub", "link", "flink"])
    def test_is_dir_error_inside_a_glob_directory_pattern(self, tmp_path, monkeypatch, rel):
        _set_syntax(monkeypatch, "glob")
        folder = tmp_path / "dags"
        folder.mkdir()
        _invariant_tree(folder, tmp_path / "outside", "glob")
        target = str(folder / rel)

        with _patchers(target)["Path.is_dir"]:
            listing = list_dag_files(str(folder))

        assert set(listing.unreadable_dirs) == {target}

    def test_own_stat_error_on_a_symlink_entry(self, tmp_path):
        folder = tmp_path / "dags"
        folder.mkdir()
        _invariant_tree(folder, tmp_path / "outside", "regexp")
        target = str(folder / "flink")

        with _patchers(target)["os.stat"]:
            listing = list_dag_files(str(folder))

        assert set(listing.unreadable_dirs) == {target}

    @pytest.mark.parametrize("rel", ["", "sub"])
    def test_open_error_on_an_ignore_file(self, tmp_path, rel):
        folder = tmp_path / "dags"
        folder.mkdir()
        _invariant_tree(folder, tmp_path / "outside", "regexp")
        directory = str(folder / rel) if rel else str(folder)

        with _patchers(os.path.join(directory, ".airflowignore"))["open"]:
            listing = list_dag_files(str(folder))

        assert set(listing.unreadable_dirs) == {directory}


# ---------------------------------------------------------------------------
# is_listed
# ---------------------------------------------------------------------------


class TestIsListed:
    def test_file_in_the_root(self, folder):
        assert is_listed(_touch(folder, "a.py"), str(folder)) is True

    def test_file_in_a_nested_directory(self, folder):
        assert is_listed(_touch(folder, "sub/deep/a.py"), str(folder)) is True

    def test_file_under_an_ignored_directory(self, folder):
        _touch(folder, ".airflowignore", "common\n")
        assert is_listed(_touch(folder, "common/factory.py"), str(folder)) is False

    def test_ignored_file(self, folder):
        _touch(folder, "sub/.airflowignore", "skip_.*\n")
        assert is_listed(_touch(folder, "sub/skip_me.py"), str(folder)) is False
        assert is_listed(_touch(folder, "sub/keep.py"), str(folder)) is True

    def test_glob_negation(self, folder, monkeypatch):
        _set_syntax(monkeypatch, "glob")
        _touch(folder, ".airflowignore", "*.tmp.py\n!keep.tmp.py\n")
        assert is_listed(_touch(folder, "keep.tmp.py"), str(folder)) is True
        assert is_listed(_touch(folder, "drop.tmp.py"), str(folder)) is False

    def test_file_outside_the_folder(self, folder, tmp_path):
        assert is_listed(_touch(tmp_path, "elsewhere/a.py"), str(folder)) is False

    def test_file_that_is_not_py(self, folder):
        assert is_listed(_touch(folder, "a.txt"), str(folder)) is False

    @pytest.mark.parametrize(
        ("syntax", "ignore"),
        [("regexp", "ignored_dir\nskip_.*\n"), ("glob", "ignored_dir/\nskip_*\n")],
    )
    def test_through_the_real_path_of_a_symlinked_folder(self, tmp_path, monkeypatch, syntax, ignore):
        _set_syntax(monkeypatch, syntax)
        real = tmp_path / "real"
        real.mkdir()
        link = tmp_path / "link"
        link.symlink_to(real)
        _touch(real, ".airflowignore", ignore)

        assert is_listed(_touch(real, "sub/a.py"), str(link)) is True
        assert is_listed(_touch(real, "sub/skip_me.py"), str(link)) is False
        assert is_listed(_touch(real, "ignored_dir/b.py"), str(link)) is False

    def test_unreadable_ignore_file_raises(self, folder, monkeypatch):
        path = _touch(folder, "sub/a.py")
        _touch(folder, "sub/.airflowignore", "x\n")
        target = str(folder / "sub/.airflowignore")

        with _patchers(target)["open"], pytest.raises(DagFolderListingError):
            is_listed(path, str(folder))

    def test_unsupported_syntax_raises(self, folder, monkeypatch):
        _set_syntax(monkeypatch, "Regexp")
        with pytest.raises(DagFolderListingError):
            is_listed(_touch(folder, "a.py"), str(folder))

    def test_invalid_glob_pattern_raises(self, folder, monkeypatch):
        _set_syntax(monkeypatch, "glob")
        _touch(folder, ".airflowignore", "\\\n")
        with pytest.raises(DagFolderListingError):
            is_listed(_touch(folder, "a.py"), str(folder))

    @pytest.mark.parametrize("syntax", ["regexp", "glob"])
    def test_agrees_with_the_listing(self, tmp_path, monkeypatch, syntax):
        _set_syntax(monkeypatch, syntax)
        folder = tmp_path / "dags"
        folder.mkdir()
        _comparison_tree(folder, tmp_path / "outside", syntax, locked=False)

        listed = list_dag_files(str(folder)).files
        py_files = [
            os.path.join(root, name)
            for root, _dirs, names in os.walk(folder, followlinks=True)
            for name in names
            if Path(name).suffix == ".py" and os.path.isfile(os.path.join(root, name))
        ]

        assert py_files
        for path in py_files:
            assert is_listed(path, str(folder)) == (path in listed), path


# ---------------------------------------------------------------------------
# Comparison with Airflow
# ---------------------------------------------------------------------------

_IGNORE_FILES = {
    "regexp": {
        ".airflowignore": "ignored_dir\nskip_.*\\.py\n# a comment line\ndrop\\.tmp\n",
        "sub/.airflowignore": "local_.*\n",
    },
    "glob": {
        ".airflowignore": "ignored_dir/\nskip_*.py\n# a comment line\n*.tmp.py\n!keep.tmp.py\n",
        "sub/.airflowignore": "local_*\n",
    },
}


def _comparison_tree(folder: Path, outside: Path, syntax: str, *, locked: bool) -> None:
    for rel, content in _IGNORE_FILES[syntax].items():
        _touch(folder, rel, content)
    for rel in (
        "a.py", ".x.py", ".hidden/h.py", "skip_me.py", "keep.tmp.py", "drop.tmp.py",
        "ignored_dir/i.py", "sub/b.py", "sub/local_c.py", "sub/deep/d.py", "notes.txt",
        ".py", "pkg.py/inner.py", "locked/l.py",
    ):
        _touch(folder, rel)
    _touch(outside, "o.py")
    (folder / "link").symlink_to(outside)
    (folder / "dead.py").symlink_to(folder / "missing.py")
    if locked:
        os.chmod(folder / "locked", 0o000)


def _airflow_py_files(folder: Path, syntax: str) -> set[str]:
    from airflow.utils.file import find_path_from_directory

    return {
        p
        for p in find_path_from_directory(str(folder), ".airflowignore", ignore_file_syntax=syntax)
        if Path(p).is_file() and Path(p).suffix == ".py"
    }


class TestAgreesWithAirflow:
    @pytest.fixture
    def tree(self, tmp_path):
        folder = tmp_path / "dags"
        folder.mkdir()
        yield folder
        if (folder / "locked").exists():
            os.chmod(folder / "locked", 0o755)

    @pytest.mark.parametrize("syntax", ["regexp", "glob"])
    def test_same_files_as_find_path_from_directory(self, tree, tmp_path, monkeypatch, syntax):
        _set_syntax(monkeypatch, syntax)
        _comparison_tree(tree, tmp_path / "outside", syntax, locked=True)

        listing = list_dag_files(str(tree))

        assert listing.files == _airflow_py_files(tree, syntax)
        assert str(tree / "a.py") in listing.files

    def test_same_files_as_list_py_file_paths_with_the_default_syntax(self, tree, tmp_path):
        from airflow.utils.file import list_py_file_paths

        _comparison_tree(tree, tmp_path / "outside", "regexp", locked=True)

        listing = list_dag_files(str(tree))

        assert listing.files == set(
            list_py_file_paths(str(tree), safe_mode=False, include_examples=False)
        )
