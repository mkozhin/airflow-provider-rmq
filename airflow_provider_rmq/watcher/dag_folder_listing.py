"""The set of DAG files in ``DAGS_FOLDER``, listed by the rules Airflow loads them by.

:func:`list_dag_files` walks the folder the way
``airflow.utils.file._find_path_from_directory`` does: ``os.walk`` with
``followlinks=True``, an ``.airflowignore`` in every directory read with the syntax
of ``[core] dag_ignore_file_syntax`` and inherited by the directories below, Airflow's
own ignore rules (``_RegexpIgnoreRule`` / ``_GlobIgnoreRule``), and a symlink loop
taken as a failure of the whole listing. Hidden files and directories are listed like
any other, as Airflow loads them.

The listing differs from Airflow's in what it does with errors and in what it keeps:

- an I/O error on a directory itself — listing it, reading its ``.airflowignore``,
  following a symlink to it — makes that directory unreadable
  (:attr:`DagFolderListing.unreadable_dirs`) and the walk goes on. Airflow's walk has
  no ``onerror``: it drops such a directory silently, or fails as a whole;
- an I/O error on a single ``.py`` entry keeps the entry in the listing, so the reader
  of the file meets the error on that file alone;
- only ``.py`` files are listed. Zip archives and the ``safe_mode`` heuristic of
  ``list_py_file_paths`` are not repeated: the watcher reads only Python source.

What makes Airflow itself fail to list the folder — a symlink loop, an unsupported
``dag_ignore_file_syntax``, an invalid glob pattern, an ``.airflowignore`` that does
not decode, a name the regexp rules cannot encode — raises
:class:`DagFolderListingError`, the only exception that leaves this module.

:func:`is_listed` answers the same question for one file, walking only the chain of
directories from ``DAGS_FOLDER`` down to it. The decorator uses it at import time.

Airflow and ``re2`` are imported inside the functions: the module is imported by the
decorator's check inside the DAG processor, where only a DAG file that already
imported Airflow reaches it.
"""
from __future__ import annotations

import errno
import os
import stat
from collections.abc import Set as AbstractSet
from pathlib import Path
from typing import Any, NamedTuple

#: Name of the ignore file Airflow reads in every directory of the DAG folder.
IGNORE_FILE_NAME = ".airflowignore"

#: ``os.stat`` errors on a symlink that say its target does not exist as a directory.
_MISSING_TARGET_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR, errno.ELOOP})


class DagFolderListing(NamedTuple):
    """Result of one walk of the DAG folder."""

    #: ``.py`` files Airflow loads, spelled ``os.path.join(root, name)`` from the folder.
    files: set[str]
    #: Directories the walk could not read, in the same spelling, with their error.
    unreadable_dirs: dict[str, Exception]
    #: Directories of the ``recheck`` argument that this walk read without error.
    recovered: set[str]


class DagFolderListingError(Exception):
    """The DAG folder cannot be listed by Airflow's rules; the message names the cause."""


def _ignore_rule_type() -> Any:
    """The Airflow ignore rule class for ``[core] dag_ignore_file_syntax``.

    The value is compared case-sensitively, as ``find_path_from_directory`` does.
    """
    try:
        from airflow.configuration import conf
        from airflow.utils.file import _GlobIgnoreRule, _RegexpIgnoreRule

        syntax = conf.get("core", "DAG_IGNORE_FILE_SYNTAX", fallback="regexp")
    except Exception as exc:
        raise DagFolderListingError(
            f"[core] dag_ignore_file_syntax cannot be read: {exc!r}"
        ) from exc
    if syntax == "glob":
        return _GlobIgnoreRule
    if syntax == "regexp" or not syntax:
        return _RegexpIgnoreRule
    raise DagFolderListingError(
        f"[core] dag_ignore_file_syntax {syntax!r} is not supported; "
        "Airflow accepts 'regexp' and 'glob'"
    )


def _with_ignore_file(directory: Path, base_dir: Path, inherited: list, rule_type: Any) -> list:
    """Rules in force in ``directory``: ``inherited`` plus its own ``.airflowignore``.

    Lines are read, stripped of comments, compiled and de-duplicated the way
    ``_find_path_from_directory`` does it. An ``OSError`` propagates as is, for the
    caller to decide what an unreadable ignore file means; any other error is one
    Airflow fails on too and becomes :class:`DagFolderListingError`.
    """
    ignore_file_path = directory / IGNORE_FILE_NAME
    if not ignore_file_path.is_file():
        return list(inherited)
    try:
        import re2

        with open(ignore_file_path) as ignore_file:
            content = ignore_file.read()
        lines = [re2.sub(r"\s*#.*", "", line) for line in content.split("\n")]
        compiled = [rule_type.compile(line, base_dir, ignore_file_path) for line in lines if line]
    except OSError:
        raise
    except Exception as exc:
        raise DagFolderListingError(f"{ignore_file_path} cannot be applied: {exc!r}") from exc
    rules = list(inherited) + [rule for rule in compiled if rule is not None]
    # The order of the rules is kept: with negation, a later rule overrides an earlier one.
    return list(dict.fromkeys(rules))


def _matches(rule_type: Any, path: Path, rules: list) -> bool:
    """Whether ``rules`` exclude ``path``; ``OSError`` propagates, the rest is fatal."""
    try:
        return bool(rule_type.match(path, rules))
    except OSError:
        raise
    except Exception as exc:
        raise DagFolderListingError(
            f"the ignore rules cannot be matched against {path}: {exc!r}"
        ) from exc


def _unidentified_dir_error(path: str) -> OSError | None:
    """The error of an entry ``os.walk`` listed as a file but that may be a directory.

    ``os.walk`` files every entry whose ``is_dir()`` raised under the files and never
    enters it: a symlink to a directory whose target cannot be reached, an entry on
    a stale or failing mount. The entry is examined again: a symlink or a directory
    whose ``stat`` fails for a reason other than a missing target is a directory the
    walk could not read. A dangling symlink or a symlink loop is not a directory.
    """
    try:
        lstat_result = os.lstat(path)
    except OSError as exc:
        return None if exc.errno == errno.ENOENT else exc
    if not (stat.S_ISLNK(lstat_result.st_mode) or stat.S_ISDIR(lstat_result.st_mode)):
        return None
    try:
        os.stat(path)
    except OSError as exc:
        return None if exc.errno in _MISSING_TARGET_ERRNOS else exc
    return None


def list_dag_files(dags_folder: str, recheck: AbstractSet[str] = frozenset()) -> DagFolderListing:
    """List the ``.py`` files of ``dags_folder`` that Airflow loads.

    ``recheck`` holds the directories an earlier listing found unreadable; those this
    walk reads without error come back in :attr:`DagFolderListing.recovered`.

    Errors are handled at three levels, and every I/O call of the walk belongs to one:

    - a directory: its ``.airflowignore`` (presence, reading) and the rules compiled
      from it. An ``OSError`` makes the directory unreadable, and none of its files or
      subdirectories is listed — what it excludes is unknown;
    - a subdirectory: matching it against the rules and resolving it for loop
      detection. An ``OSError`` makes that subdirectory unreadable;
    - an entry: matching it and checking that it is a file. An ``OSError`` on a
      ``.py`` entry keeps it in the listing; on any other entry it makes the entry an
      unreadable directory (see :func:`_unidentified_dir_error`).

    Raises :class:`DagFolderListingError` where Airflow's walk fails as a whole.
    """
    rule_type = _ignore_rule_type()
    base_dir = Path(dags_folder)
    files: set[str] = set()
    unreadable_dirs: dict[str, Exception] = {}
    recovered: set[str] = set()
    # Rules inherited by each directory, keyed by its resolved path, as in Airflow.
    patterns_by_dir: dict[Path, list] = {}

    def on_walk_error(exc: OSError) -> None:
        unreadable_dirs[str(exc.filename) if exc.filename is not None else dags_folder] = exc

    for root, dirs, names in os.walk(dags_folder, followlinks=True, onerror=on_walk_error):
        root_path = Path(root)

        # Directory level.
        try:
            inherited = patterns_by_dir.get(root_path.resolve(), [])
            patterns = _with_ignore_file(root_path, base_dir, inherited, rule_type)
        except OSError as exc:
            unreadable_dirs[root] = exc
            dirs[:] = []
            continue
        except RuntimeError as exc:
            raise DagFolderListingError(f"{root} cannot be resolved: {exc!r}") from exc
        if root in recheck:
            recovered.add(root)

        # Subdirectory level.
        kept: list[str] = []
        for subdir in dirs:
            subdir_path = root_path / subdir
            try:
                if _matches(rule_type, subdir_path, patterns):
                    continue
                resolved = subdir_path.resolve()
            except OSError as exc:
                unreadable_dirs[os.path.join(root, subdir)] = exc
                continue
            except RuntimeError as exc:
                raise DagFolderListingError(f"{subdir_path} cannot be resolved: {exc!r}") from exc
            if resolved in patterns_by_dir:
                raise DagFolderListingError(
                    f"the DAG folder {dags_folder} has a symlink loop: {resolved} is "
                    "reached more than once"
                )
            patterns_by_dir[resolved] = list(patterns)
            kept.append(subdir)
        dirs[:] = kept

        # Entry level.
        for name in names:
            if name == IGNORE_FILE_NAME:
                continue
            entry = os.path.join(root, name)
            entry_path = Path(entry)
            is_py = entry_path.suffix == ".py"
            try:
                if _matches(rule_type, entry_path, patterns):
                    continue
                if is_py:
                    if entry_path.is_file():
                        files.add(entry)
                    continue
            except OSError as exc:
                if is_py:
                    # The reader of the file meets the error on this file alone.
                    files.add(entry)
                else:
                    unreadable_dirs[entry] = exc
                continue
            error = _unidentified_dir_error(entry)
            if error is not None:
                unreadable_dirs[entry] = error

    return DagFolderListing(files, unreadable_dirs, recovered)


def is_listed(path: str, dags_folder: str) -> bool:
    """Whether :func:`list_dag_files` of ``dags_folder`` lists the file at ``path``.

    The file must be a ``.py`` file inside the folder. The two paths are compared
    through ``os.path.abspath`` first and, when that does not place the file inside
    the folder, through ``os.path.realpath`` of both. The spelling that matched is
    used for the whole check — the base of the regexp rules, the chain of
    directories and the file — so every path the rules see lies under their base.

    Each directory from the folder down to the file's directory adds the rules of
    its ``.airflowignore`` to those it inherits and must not be excluded by the rules
    of its parent; the file must not be excluded by the rules of its directory.

    Any error — an unreadable ``.airflowignore``, an I/O error inside the rules, an
    invalid rule, an unsupported syntax — raises :class:`DagFolderListingError`.
    """
    if Path(path).suffix != ".py":
        return False
    for spell in (os.path.abspath, os.path.realpath):
        folder = spell(dags_folder)
        file_path = spell(path)
        if file_path.startswith(os.path.join(folder, "")):
            break
    else:
        return False

    rule_type = _ignore_rule_type()
    base_dir = Path(folder)
    target = Path(file_path)
    try:
        directory = base_dir
        rules = _with_ignore_file(directory, base_dir, [], rule_type)
        for part in target.relative_to(base_dir).parts[:-1]:
            directory = directory / part
            if _matches(rule_type, directory, rules):
                return False
            rules = _with_ignore_file(directory, base_dir, rules, rule_type)
        return not _matches(rule_type, target, rules)
    except OSError as exc:
        raise DagFolderListingError(
            f"the ignore rules for {file_path} cannot be read: {exc!r}"
        ) from exc
