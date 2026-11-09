"""Where does a table live? One answer, used by the pipeline and every job.

Shared (dev/prod) targets split schemas by layer, domain and PII:

    raw_<domain>          trusted_<domain>          public, PII hashed
    pii_raw_<domain>      pii_trusted_<domain>      vaulted, PII in clear text

A sandbox target puts everything in one personal schema and never builds a
vault, so developers never see clear-text PII.
"""

from __future__ import annotations

from dataclasses import dataclass

LAYERS = ("raw", "trusted")


@dataclass(frozen=True)
class Naming:
    env: str
    catalog: str
    sandbox_schema: str = ""
    raw_pattern: str = "raw_{domain}"
    trusted_pattern: str = "trusted_{domain}"
    pii_raw_pattern: str = "pii_raw_{domain}"
    pii_trusted_pattern: str = "pii_trusted_{domain}"

    @property
    def is_sandbox(self) -> bool:
        return self.env == "sandbox"

    @property
    def builds_vault(self) -> bool:
        return not self.is_sandbox

    def schema(self, layer: str, domain: str, vaulted: bool = False) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer '{layer}'")
        if self.is_sandbox:
            if not self.sandbox_schema:
                raise ValueError("sandbox target needs a sandbox_schema")
            return self.sandbox_schema
        if vaulted:
            pattern = self.pii_raw_pattern if layer == "raw" else self.pii_trusted_pattern
        else:
            pattern = self.raw_pattern if layer == "raw" else self.trusted_pattern
        return pattern.format(domain=domain).lower()

    def fqn(self, layer: str, domain: str, table: str, vaulted: bool = False) -> str:
        """Fully qualified name: catalog.schema.table."""
        return f"{self.catalog}.{self.schema(layer, domain, vaulted)}.{table}"

    def all_schemas(self, domains: set[str], pii_domains: set[str]) -> list[str]:
        """Every schema the pipeline publishes to (used by the setup job)."""
        if self.is_sandbox:
            return [self.sandbox_schema]
        out = {self.schema(layer, d) for layer in LAYERS for d in domains}
        out |= {self.schema(layer, d, vaulted=True) for layer in LAYERS for d in pii_domains}
        return sorted(out)
