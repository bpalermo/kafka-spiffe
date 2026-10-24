"""Linter aspects, enabled by `--config=lint` (see .bazelrc)."""

load("@aspect_rules_lint//lint:pmd.bzl", "lint_pmd_aspect")

# Java: visits java_library and java_binary, so tests (java_test) are not linted.
# Generated sources are skipped. Error Prone and -Xlint run in the compiler; the
# ruleset keeps to what they do not report.
pmd = lint_pmd_aspect(
    binary = Label("//bazel/lint:pmd"),
    rulesets = [Label("//:pmd.xml")],
)
