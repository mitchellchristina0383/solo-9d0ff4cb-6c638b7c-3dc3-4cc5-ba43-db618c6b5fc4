"""FastAPI application: a local HTTP surface over the aggregation engine."""

from __future__ import annotations

import contextlib
import logging
from typing import Iterator

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from .config import Settings, load_settings
from .engine import BadRequest, Engine, JobBusy, TrialConflict, TrialNotConfirmable
from .models import (
    DiversionConfirmRequest,
    DiversionTrial,
    DiversionTrialRequest,
    DiversionTrialSummary,
    ErrorResponse,
    JobCreateRequest,
    JobRunResponse,
    JobState,
    JobSummary,
    MarkSourcesUnavailableRequest,
    RestoreSourcesRequest,
    RestoreSourcesResponse,
    ResultsResponse,
    RunRequest,
    SourceShardInfo,
)
from .storage import InvalidName, JobNotFound, StorageError, TrialNotFound

logger = logging.getLogger("gateway_log_aggregator")

_ERRORS = {
    JobNotFound: status.HTTP_404_NOT_FOUND,
    TrialNotFound: status.HTTP_404_NOT_FOUND,
    InvalidName: status.HTTP_422_UNPROCESSABLE_ENTITY,
    BadRequest: status.HTTP_400_BAD_REQUEST,
    TrialNotConfirmable: status.HTTP_400_BAD_REQUEST,
    TrialConflict: status.HTTP_409_CONFLICT,
    JobBusy: status.HTTP_409_CONFLICT,
}


def create_app(settings: Settings | None = None) -> FastAPI:
    configured = settings or load_settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> Iterator[None]:
        app.state.engine = Engine(configured)
        logger.info(
            "engine ready data_dir=%s work_dir=%s", configured.data_dir, configured.work_dir
        )
        yield

    app = FastAPI(
        title="Gateway Log Aggregator",
        version="1.1.0",
        description=(
            "Sums gateway access-log shards by domain (request totals and 5xx counts).\n\n"
            "A job owns a fixed set of source shards. Each shard is scanned with a single "
            "streaming pass and written as per-domain temporary count files, which are then "
            "merged.\n\n"
            "**Repair flow**: run the job once, then call `POST /jobs/{job_id}/run` again "
            "with `fault_injection` entries that delete/truncate a temporary part file or "
            "mark a source unavailable. A part damaged in the request is detected and its "
            "source is re-read in the *same* run (and reported in `reread_shards` / "
            "`reread_shard_numbers`). Damage found at the start of a later run (parts lost "
            "between runs) is repaired by re-reading only the owning sources before merge. "
            "A source that stays unreadable withholds its partitions instead of publishing "
            "a half sum; restore it and call `/run` again to retry.\n\n"
            "**Diversion trial**: pick a completed job and the gateway to take offline. "
            "Give every remaining gateway its period request limit and, per affected "
            "domain, the gateways that may receive it. Limits are reduced by all requests "
            "the gateway already serves; the rest is shared by all domains. The trial "
            "returns the maximum number of migratable requests, the per-domain split and "
            "the leftover, or only the missing items when the summary is incomplete. A "
            "fully divertible trial can be confirmed only while the published per-domain "
            "per-shard counts are unchanged.\n\n"
            "Use `GET /sources` to list shard file names available to jobs."
        ),
        lifespan=lifespan,
        responses={500: {"model": ErrorResponse}},
    )

    def get_engine(request: Request) -> Engine:
        engine: Engine | None = getattr(request.app.state, "engine", None)
        if engine is None:  # pragma: no cover - lifespan always sets it
            raise HTTPException(status_code=503, detail="engine is not ready")
        return engine

    async def handle_errors(request: Request, exc: Exception) -> JSONResponse:
        if isinstance(exc, StorageError) and not isinstance(
            exc, (JobNotFound, TrialNotFound, InvalidName)
        ):
            logger.exception("storage failure")
            return JSONResponse(status_code=500, content={"detail": str(exc)})
        for klass, code in _ERRORS.items():
            if isinstance(exc, klass):
                body = {"detail": str(exc)}
                error_code = getattr(exc, "code", None)
                if error_code:
                    body["code"] = error_code
                    body["recalculate"] = error_code in (
                        "results_changed",
                        "summary_incomplete_now",
                    )
                return JSONResponse(status_code=code, content=body)
        logger.exception("unhandled error")
        return JSONResponse(status_code=500, content={"detail": "internal error"})

    for exc_type in (
        JobNotFound,
        TrialNotFound,
        InvalidName,
        BadRequest,
        TrialNotConfirmable,
        TrialConflict,
        JobBusy,
        StorageError,
    ):
        app.add_exception_handler(exc_type, handle_errors)

    @app.get("/health", tags=["ops"], summary="Liveness probe")
    def health(engine: Engine = Depends(get_engine)) -> dict:
        return {
            "status": "ok",
            "data_dir": str(engine.settings.data_dir),
            "data_dir_exists": engine.settings.data_dir.is_dir(),
            "work_dir": str(engine.settings.work_dir),
            "work_dir_writable": _writable(engine.settings.work_dir),
        }

    @app.get(
        "/sources",
        response_model=list[SourceShardInfo],
        tags=["sources"],
        summary="List source shard files under the data directory",
    )
    def list_sources(engine: Engine = Depends(get_engine)) -> list[SourceShardInfo]:
        return engine.list_sources()

    @app.post(
        "/jobs",
        response_model=JobState,
        status_code=status.HTTP_201_CREATED,
        tags=["jobs"],
        summary="Create a job with a fixed set of source shards",
        responses={400: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
    )
    def create_job(
        payload: JobCreateRequest, engine: Engine = Depends(get_engine)
    ) -> JobState:
        return engine.create_job(payload)

    @app.get("/jobs", response_model=list[JobSummary], tags=["jobs"], summary="List jobs")
    def list_jobs(engine: Engine = Depends(get_engine)) -> list[JobSummary]:
        return engine.list_jobs()

    @app.get(
        "/jobs/{job_id}",
        response_model=JobState,
        tags=["jobs"],
        summary="Fetch full job state",
        responses={404: {"model": ErrorResponse}},
    )
    def get_job(job_id: str, engine: Engine = Depends(get_engine)) -> JobState:
        return engine.get_job(job_id)

    @app.post(
        "/jobs/{job_id}/run",
        response_model=JobRunResponse,
        tags=["jobs"],
        summary="Run (or retry/repair) a job; returns the run report with re-read shards",
        responses={
            400: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
        },
    )
    def run_job(
        job_id: str,
        payload: RunRequest | None = None,
        engine: Engine = Depends(get_engine),
    ) -> JobRunResponse:
        return engine.run_job(job_id, payload or RunRequest())

    @app.get(
        "/jobs/{job_id}/results",
        response_model=ResultsResponse,
        tags=["results"],
        summary="Read published per-domain results; blocked partitions are listed separately",
        responses={404: {"model": ErrorResponse}},
    )
    def get_results(job_id: str, engine: Engine = Depends(get_engine)) -> ResultsResponse:
        return engine.get_results(job_id)

    @app.post(
        "/jobs/{job_id}/sources/unavailable",
        tags=["simulation"],
        summary="Demo: mark source shards unavailable for this job",
        responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    )
    def mark_unavailable(
        job_id: str,
        payload: MarkSourcesUnavailableRequest,
        engine: Engine = Depends(get_engine),
    ) -> dict:
        _require_simulation(engine)
        unavailable = engine.mark_unavailable(job_id, payload.shard_ids)
        return {"job_id": job_id, "unavailable": unavailable}

    @app.post(
        "/jobs/{job_id}/sources/restore",
        response_model=RestoreSourcesResponse,
        tags=["simulation"],
        summary="Demo: restore simulated source outages, then POST /run to retry",
        responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    )
    def restore_sources(
        job_id: str,
        payload: RestoreSourcesRequest,
        engine: Engine = Depends(get_engine),
    ) -> RestoreSourcesResponse:
        _require_simulation(engine)
        return engine.restore_sources(job_id, None if payload.all else payload.shard_ids)

    @app.post(
        "/jobs/{job_id}/diversion-trials",
        response_model=DiversionTrial,
        status_code=status.HTTP_201_CREATED,
        tags=["diversion"],
        summary="Calculate a diversion trial: move one gateway's requests to the remaining gateways",
        responses={
            400: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
        },
    )
    def create_diversion_trial(
        job_id: str,
        payload: DiversionTrialRequest,
        engine: Engine = Depends(get_engine),
    ) -> DiversionTrial:
        return engine.create_trial(job_id, payload)

    @app.get(
        "/jobs/{job_id}/diversion-trials",
        response_model=list[DiversionTrialSummary],
        tags=["diversion"],
        summary="List diversion trials of a job",
        responses={404: {"model": ErrorResponse}},
    )
    def list_diversion_trials(
        job_id: str, engine: Engine = Depends(get_engine)
    ) -> list[DiversionTrialSummary]:
        return engine.list_trials(job_id)

    @app.get(
        "/jobs/{job_id}/diversion-trials/{trial_id}",
        response_model=DiversionTrial,
        tags=["diversion"],
        summary="Fetch a diversion trial (missing items and per-domain assignment plan)",
        responses={404: {"model": ErrorResponse}},
    )
    def get_diversion_trial(
        job_id: str, trial_id: str, engine: Engine = Depends(get_engine)
    ) -> DiversionTrial:
        return engine.get_trial(job_id, trial_id)

    @app.post(
        "/jobs/{job_id}/diversion-trials/{trial_id}/confirm",
        response_model=DiversionTrial,
        tags=["diversion"],
        summary="Confirm a fully divertible trial if the published counts are unchanged",
        responses={
            400: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
        },
    )
    def confirm_diversion_trial(
        job_id: str,
        trial_id: str,
        payload: DiversionConfirmRequest | None = None,
        engine: Engine = Depends(get_engine),
    ) -> DiversionTrial:
        return engine.confirm_trial(job_id, trial_id, payload or DiversionConfirmRequest())

    @app.get("/", include_in_schema=False)
    def root() -> dict:
        return {"service": "gateway-log-aggregator", "docs": "/docs", "health": "/health"}

    return app


def _writable(path) -> bool:  # pragma: no cover - trivial
    import tempfile

    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path):
            return True
    except OSError:
        return False


def _require_simulation(engine: Engine) -> None:
    if not engine.settings.allow_simulation:
        raise HTTPException(
            status_code=403,
            detail=(
                "simulation endpoints are disabled; start the service with "
                "ALLOW_SIMULATION=true to use them (run-level fault_injection still works)"
            ),
        )


app = create_app()
