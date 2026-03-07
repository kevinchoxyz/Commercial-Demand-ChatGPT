"""Validation report structures."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    level: str = "error"
    context: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationReport:
    issues: tuple[ValidationIssue, ...] = tuple()

    @property
    def has_errors(self) -> bool:
        return any(issue.level == "error" for issue in self.issues)

    def extend(self, issues: list[ValidationIssue]) -> "ValidationReport":
        return ValidationReport(issues=self.issues + tuple(issues))

