#!/usr/bin/env python3
"""lineage_client.py — OpenLineage client utility with File Transport.

MissHoover V2: Data-Centric Determinism with OpenLineage
- Uses FileTransport to write lineage events to docs/lineage.jsonl
- Provides helper functions to emit RunEvent (START, COMPLETE, FAIL)
- Registers Dataset inputs/outputs for data provenance tracking

Usage:
    from scripts.utils.lineage_client import LineageClient
    
    client = LineageClient()
    client.emit_start("phase-3-pipeline", inputs=[...], outputs=[...])
    client.emit_complete("phase-3-pipeline")
    # or
    client.emit_fail("phase-3-pipeline", error="Schema mismatch")
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# OpenLineage imports - will fail gracefully if not installed
try:
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import Job, Run, RunEvent, RunState
    from openlineage.client.transport.file import FileTransport
    from openlineage.client.facet import (
        SourceCodeJobFacet,
        DocumentationJobFacet,
        DataQualityMetricsInputDatasetFacet,
    )
    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False
    OpenLineageClient = None
    FileTransport = None

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LINEAGE_FILE = REPO_ROOT / "docs" / "lineage.jsonl"
STATE_FILE = REPO_ROOT / "docs" / "ralph-state.json"


class LineageClient:
    """OpenLineage client with File Transport for MissHoover V2.
    
    Emits lineage events to docs/lineage.jsonl in OpenLineage format.
    This creates a traceable record of all data transformations.
    """
    
    def __init__(
        self,
        namespace: str = "misshoover://pea-met-network",
        producer: str = "misshoover-v2",
    ):
        self.namespace = namespace
        self.producer = producer
        self._client = None
        self._current_run_id: str | None = None
        self._current_job_name: str | None = None
        
        if OPENLINEAGE_AVAILABLE and FileTransport:
            # Configure FileTransport via environment variable
            os.environ["OPENLINEAGE_TRANSPORT"] = json.dumps({
                "type": "file",
                "log_file_path": str(LINEAGE_FILE),
            })
            self._client = OpenLineageClient()
        else:
            # Fallback: write JSONL directly
            self._client = None
    
    def _ensure_lineage_file(self) -> None:
        """Ensure lineage file exists."""
        LINEAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return str(uuid.uuid4())
    
    def _get_timestamp(self) -> str:
        """Get current ISO timestamp."""
        return datetime.now(timezone.utc).isoformat()
    
    def _load_state(self) -> dict:
        """Load ralph-state.json for context."""
        if STATE_FILE.exists():
            try:
                return json.loads(STATE_FILE.read_text())
            except json.JSONDecodeError:
                pass
        return {}
    
    def _write_event_direct(self, event: dict) -> None:
        """Write event directly to JSONL file (fallback mode)."""
        self._ensure_lineage_file()
        with open(LINEAGE_FILE, "a") as f:
            f.write(json.dumps(event) + "\n")
    
    def _build_dataset(
        self,
        name: str,
        namespace: str | None = None,
        facets: dict | None = None,
    ) -> dict:
        """Build an OpenLineage Dataset representation."""
        return {
            "namespace": namespace or self.namespace,
            "name": name,
            "facets": facets or {},
        }
    
    def _build_job(self, name: str, facets: dict | None = None) -> dict:
        """Build an OpenLineage Job representation."""
        return {
            "namespace": self.namespace,
            "name": name,
            "facets": facets or {},
        }
    
    def emit_start(
        self,
        job_name: str,
        inputs: list[dict] | None = None,
        outputs: list[dict] | None = None,
        description: str | None = None,
    ) -> str:
        """Emit a START run event.
        
        Args:
            job_name: Name of the job/phase (e.g., "phase-3-pipeline")
            inputs: List of input datasets with 'name' and optional 'namespace'
            outputs: List of output datasets
            description: Optional job description
            
        Returns:
            The run ID for subsequent events
        """
        run_id = self._generate_run_id()
        self._current_run_id = run_id
        self._current_job_name = job_name
        
        state = self._load_state()
        phase = state.get("phase", "?")
        
        event = {
            "eventType": "START",
            "eventTime": self._get_timestamp(),
            "run": {
                "runId": run_id,
                "facets": {
                    "misshoover": {
                        "phase": phase,
                        "iteration": state.get("iteration", 0),
                        "producer": self.producer,
                    }
                },
            },
            "job": self._build_job(
                job_name,
                facets={
                    "documentation": {"description": description or f"MissHoover phase {phase}"},
                    "sourceCodeLocation": {"url": f"file://{REPO_ROOT}"},
                }
            ),
            "inputs": [self._build_dataset(**i) for i in (inputs or [])],
            "outputs": [self._build_dataset(**o) for o in (outputs or [])],
            "producer": self.producer,
        }
        
        if self._client and OPENLINEAGE_AVAILABLE:
            # Use OpenLineage client
            try:
                run = Run(runId=run_id)
                job = Job(namespace=self.namespace, name=job_name)
                run_event = RunEvent(
                    eventType=RunState.START,
                    eventTime=datetime.now(timezone.utc),
                    run=run,
                    job=job,
                    producer=self.producer,
                )
                self._client.emit(run_event)
            except Exception:
                # Fallback to direct write
                self._write_event_direct(event)
        else:
            self._write_event_direct(event)
        
        return run_id
    
    def emit_complete(
        self,
        job_name: str | None = None,
        run_id: str | None = None,
        outputs: list[dict] | None = None,
    ) -> None:
        """Emit a COMPLETE run event.
        
        Args:
            job_name: Job name (uses current if not provided)
            run_id: Run ID (uses current if not provided)
            outputs: Additional outputs discovered during run
        """
        job_name = job_name or self._current_job_name or "unknown"
        run_id = run_id or self._current_run_id or self._generate_run_id()
        
        state = self._load_state()
        phase = state.get("phase", "?")
        
        event = {
            "eventType": "COMPLETE",
            "eventTime": self._get_timestamp(),
            "run": {
                "runId": run_id,
                "facets": {
                    "misshoover": {
                        "phase": phase,
                        "iteration": state.get("iteration", 0),
                        "status": "success",
                    }
                },
            },
            "job": self._build_job(job_name),
            "outputs": [self._build_dataset(**o) for o in (outputs or [])],
            "producer": self.producer,
        }
        
        if self._client and OPENLINEAGE_AVAILABLE:
            try:
                run = Run(runId=run_id)
                job = Job(namespace=self.namespace, name=job_name)
                run_event = RunEvent(
                    eventType=RunState.COMPLETE,
                    eventTime=datetime.now(timezone.utc),
                    run=run,
                    job=job,
                    producer=self.producer,
                )
                self._client.emit(run_event)
            except Exception:
                self._write_event_direct(event)
        else:
            self._write_event_direct(event)
        
        # Reset current run
        self._current_run_id = None
        self._current_job_name = None
    
    def emit_fail(
        self,
        job_name: str | None = None,
        run_id: str | None = None,
        error: str | None = None,
        failing_nodes: list[dict] | None = None,
    ) -> None:
        """Emit a FAIL run event.
        
        Args:
            job_name: Job name (uses current if not provided)
            run_id: Run ID (uses current if not provided)
            error: Error message
            failing_nodes: Structured failure information (file, line, message)
        """
        job_name = job_name or self._current_job_name or "unknown"
        run_id = run_id or self._current_run_id or self._generate_run_id()
        
        state = self._load_state()
        phase = state.get("phase", "?")
        
        event = {
            "eventType": "FAIL",
            "eventTime": self._get_timestamp(),
            "run": {
                "runId": run_id,
                "facets": {
                    "misshoover": {
                        "phase": phase,
                        "iteration": state.get("iteration", 0),
                        "status": "failed",
                        "error": error,
                        "failing_nodes": failing_nodes or [],
                    }
                },
            },
            "job": self._build_job(job_name),
            "producer": self.producer,
        }
        
        if self._client and OPENLINEAGE_AVAILABLE:
            try:
                run = Run(runId=run_id)
                job = Job(namespace=self.namespace, name=job_name)
                run_event = RunEvent(
                    eventType=RunState.FAIL,
                    eventTime=datetime.now(timezone.utc),
                    run=run,
                    job=job,
                    producer=self.producer,
                )
                self._client.emit(run_event)
            except Exception:
                self._write_event_direct(event)
        else:
            self._write_event_direct(event)
        
        # Reset current run
        self._current_run_id = None
        self._current_job_name = None
    
    def register_dataset(
        self,
        name: str,
        path: str,
        schema: dict | None = None,
        input_facets: dict | None = None,
    ) -> dict:
        """Create a dataset registration for lineage tracking.
        
        Args:
            name: Dataset name (e.g., "cavendish/station_hourly")
            path: File path relative to repo root
            schema: Optional schema information
            input_facets: Optional additional facets
            
        Returns:
            Dataset dict suitable for inputs/outputs
        """
        facets = {}
        if schema:
            facets["schema"] = schema
        if input_facets:
            facets.update(input_facets)
        
        return {
            "name": name,
            "namespace": f"file://{REPO_ROOT}",
            "facets": facets,
        }


def emit_lineage_event(
    event_type: str,
    job_name: str,
    inputs: list[dict] | None = None,
    outputs: list[dict] | None = None,
    error: str | None = None,
    failing_nodes: list[dict] | None = None,
) -> str | None:
    """Convenience function to emit a lineage event.
    
    Args:
        event_type: START, COMPLETE, or FAIL
        job_name: Name of the job
        inputs: Input datasets
        outputs: Output datasets
        error: Error message (for FAIL)
        failing_nodes: Structured failure info (for FAIL)
        
    Returns:
        Run ID for START events, None otherwise
    """
    client = LineageClient()
    
    if event_type == "START":
        return client.emit_start(job_name, inputs=inputs, outputs=outputs)
    elif event_type == "COMPLETE":
        client.emit_complete(job_name, outputs=outputs)
        return None
    elif event_type == "FAIL":
        client.emit_fail(job_name, error=error, failing_nodes=failing_nodes)
        return None
    else:
        raise ValueError(f"Unknown event type: {event_type}")


if __name__ == "__main__":
    # Demo/test usage
    print("Testing LineageClient...")
    client = LineageClient()
    
    run_id = client.emit_start(
        "test-job",
        inputs=[{"name": "data/raw/input.csv"}],
        outputs=[{"name": "data/processed/output.csv"}],
        description="Test job for lineage tracking",
    )
    print(f"Started run: {run_id}")
    
    client.emit_complete("test-job", run_id)
    print("Completed run")
    
    # Check lineage file
    if LINEAGE_FILE.exists():
        print(f"\nLineage events in {LINEAGE_FILE}:")
        for line in LINEAGE_FILE.read_text().strip().split("\n"):
            event = json.loads(line)
            print(f"  {event['eventType']}: {event['job']['name']}")
