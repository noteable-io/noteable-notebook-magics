import enum
import itertools
from typing import List, Optional

from git import Diff, InvalidGitRepositoryError, Repo
from pydantic import BaseModel
from unidiff import PatchedFile, PatchSet

from .util import removeprefix


@enum.unique
class ChangeType(enum.Enum):
    # Addition of a file
    ADDED = "A"
    # Copy of a file into a new one
    COPIED = "C"
    # Deletion of a file
    DELETED = "D"
    # Modification of the contents or mode of a file
    MODIFIED = "M"
    # Renaming of a file
    RENAMED = "R"
    # Change in the type of file (symlink to non-symlink)
    TYPE_CHANGED = "T"
    # File is unmerged (you must complete the merge before it can be committed)
    UNMERGED = "U"
    # "Unknown" change type (most probably a bug, please report it)
    UNKNOWN = "X"


class ChangeSummary(BaseModel):
    """
    There are a few cases where None has to be expected as a
    member variable value:

    New File:
    a_path is None

    Deleted File:
    b_path is None
    """

    type: ChangeType
    a_path: Optional[str]
    b_path: Optional[str]

    @property
    def path(self) -> str:
        if self.a_path and self.b_path:
            if self.a_path != self.b_path:
                return f"{self.b_path} -> {self.a_path}"
            return self.a_path
        return self.a_path or self.b_path

    @classmethod
    def from_diff(cls, diff: Diff) -> "ChangeSummary":
        return cls(
            type=ChangeType(diff.change_type),
            a_path=diff.a_path,
            b_path=diff.b_path,
        )


class DiffChange(BaseModel):
    type: ChangeType
    a_path: str
    b_path: str
    a_lines: List[str]
    b_lines: List[str]

    @classmethod
    def from_patched_file(cls, patched: PatchedFile) -> "DiffChange":
        return cls(
            type=ChangeType.RENAMED if patched.is_rename else ChangeType.MODIFIED,
            a_path=removeprefix(patched.source_file, "a/"),
            b_path=removeprefix(patched.target_file, "b/"),
            a_lines=list(itertools.chain(*[x.source for x in patched])),
            b_lines=list(itertools.chain(*[x.target for x in patched])),
        )


class GitStatus(BaseModel):
    untracked_files: List[str]
    changes_not_staged_for_commit: List[ChangeSummary]
    changes_staged_for_commit: List[ChangeSummary]

    def has_changes(self) -> bool:
        return (
            len(self.untracked_files) > 0
            or len(self.changes_not_staged_for_commit) > 0
            or len(self.changes_staged_for_commit) > 0
        )


class GitDiff(BaseModel):
    changes: List[DiffChange]
    raw: str

    def has_changes(self) -> bool:
        return len(self.changes) > 0


class GitInit(BaseModel):
    created: bool


class GitUser(BaseModel):
    name: str
    email: str


class GitService:
    def __init__(self, path: str, user: GitUser) -> None:
        self._cwd = path
        self._user = user

        try:
            self._repo = Repo(path)
        except InvalidGitRepositoryError:
            self._repo = None

    @property
    def repo(self) -> Repo:
        if self._repo is None:
            self.init()
        return self._repo

    def status(self) -> GitStatus:
        return GitStatus(
            untracked_files=self.repo.untracked_files,
            changes_staged_for_commit=[
                ChangeSummary.from_diff(d) for d in self.repo.index.diff("HEAD")
            ],
            changes_not_staged_for_commit=[
                ChangeSummary.from_diff(d) for d in self.repo.index.diff(None)
            ],
        )

    def diff(self) -> GitDiff:
        raw_diff = self.repo.git.diff()
        patch_set = PatchSet(raw_diff)
        return GitDiff(changes=[DiffChange.from_patched_file(p) for p in patch_set], raw=raw_diff)

    def init(self) -> GitInit:
        if self._repo is None:
            self._repo = Repo.init(self._cwd, mkdir=False)

            # Set user name & email config values before committing
            with self._repo.config_writer() as config:
                config.set_value("user", "name", self._user.name)
                config.set_value("user", "email", self._user.email)

            self.add_and_commit_all("Initial project setup")
            return GitInit(created=True)
        return GitInit(created=False)

    def add_and_commit_all(self, message: Optional[str] = None):
        self.repo.git.add(A=True)
        self.repo.git.commit("-m", message or "updated project files", "--allow-empty")
