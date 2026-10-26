"""Regression tests for the architecture synchronization gate (stdlib unittest)."""
from __future__ import annotations

import unittest

import check_architecture as gate


class ArchitectureGateTests(unittest.TestCase):
    def test_every_current_rust_source_has_a_reviewed_responsibility(self) -> None:
        self.assertEqual(set(gate.collect_sources()), set(gate.SOURCE_RESPONSIBILITIES))

    def test_required_evidence_symbols_exist_in_test_inventory(self) -> None:
        symbols = {name for _, name, _ in gate.collect_tests()}
        self.assertLessEqual(gate.REQUIRED_TESTS, symbols)

    def test_architecture_has_all_structurally_validated_diagrams(self) -> None:
        document = gate.DOC.read_text(encoding="utf-8")
        self.assertEqual([], gate.check_mermaid_structure(document))
        self.assertEqual(8, len(gate.MERMAID_RE.findall(document)))

    def test_structural_checker_rejects_undeclared_nodes(self) -> None:
        invalid = "```mermaid\nflowchart LR\nA[\"Declared\"] --> B\n```"
        errors = gate.check_mermaid_structure(invalid)
        self.assertTrue(any("undeclared node B" in error for error in errors), errors)

    def test_structural_checker_rejects_unparsed_statements(self) -> None:
        invalid = "```mermaid\nflowchart LR\nA[\"Declared\"]\nnot a Mermaid statement\n```"
        errors = gate.check_mermaid_structure(invalid)
        self.assertTrue(any("unsupported/unparsed Mermaid statement" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
