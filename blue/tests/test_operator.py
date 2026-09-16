import copy, json, os, stat
from pathlib import Path
from unittest.mock import AsyncMock
import pytest, yaml
from package_redis_operator_blue import (
    tools as t,
    workflow as w,
    operator as op,
    adapter as a,
    probe as p,
)
from blue.workflow import run

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def opts(tmp_path):
    return {
        **w.DEFAULTS,
        **yaml.safe_load((ROOT / "test/fixtures/colors.yml").read_text()),
        "resource-name": "redis-operator-fixture",
        "workdir": str(tmp_path),
    }


def resource():
    return {
        "metadata": {"uid": "uid", "generation": 2, "resourceVersion": "10"},
        "spec": {"config": {"profile": "fixture"}},
        "status": {
            "phase": "Ready",
            "observedGeneration": 2,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


async def test_build_and_dry_run(opts, monkeypatch):
    monkeypatch.setattr(
        t, "kubectl", AsyncMock(side_effect=AssertionError("side effect"))
    )
    for event in ["build", "create"]:
        result = await run(
            w.WORKFLOW, {**opts, "blue/event": event, "blue/dry-run": True}
        )
        assert result["blue/exit"] == 0
    assert Path(opts["workdir"], opts["profile"], "operator/manifests.json").exists()


@pytest.mark.parametrize("event", ["delete", "drill"])
async def test_guards_even_dry_run(opts, event):
    result = await w.start_step({**opts, "blue/event": event, "blue/dry-run": True}, {})
    assert result["blue/exit"] == 2


async def test_profile_guard_and_credentials(opts):
    result = await w.start_step(
        {**opts, "blue/event": "build"}, {"COLORS_PAR_PROFILE": "other"}
    )
    assert result["blue/exit"] == 2
    result = await w.start_step({**opts, "blue/event": "create"}, {})
    assert result["blue/exit"] == 2


@pytest.mark.parametrize(
    "source",
    [
        "127.0.0.1/32",
        "10.0.0.1/32",
        "100.64.0.1/32",
        "192.0.2.1/32",
        "224.0.0.1/32",
        "8.8.8.8/24",
        "8.8.8.8",
    ],
)
def test_private_sources(opts, source):
    assert w.state_errors({**opts, "digitalocean-ssh-sources": [source]})


def test_render_matches_green(opts):
    for name, value in [
        ("manifests.json", t.manifests(opts)),
        ("redis-deployment.json", t.resource(opts)),
    ]:
        assert value == json.loads(
            (
                ROOT / "test/resources/golden/redis-operator-fixture/operator" / name
            ).read_text()
        )
    assert "suspend" not in t.resource(opts)["spec"]


def test_generation_and_acknowledgement():
    cr = resource()
    assert t.ready(cr)
    cr["metadata"]["generation"] = 3
    assert not t.ready(cr)
    cr["spec"]["suspend"] = True
    cr["status"].update(phase="Suspended", observedGeneration=3)
    assert t.acknowledged(cr)
    cr["metadata"]["deletionTimestamp"] = "now"
    assert not t.acknowledged(cr)


def test_ownership():
    observed = {
        "providerId": "12",
        "name": "fixture",
        "profile": "fixture",
        "ip": "8.8.8.8",
    }
    droplet = {
        "id": 12,
        "name": "fixture",
        "tags": [],
        "networks": {"v4": [{"type": "public", "ip_address": "8.8.8.8"}]},
    }
    assert t.owned_droplet(droplet, observed, "fixture", [])
    for bad in [
        {**droplet, "id": 13},
        {**droplet, "name": "other"},
        {**droplet, "tags": ["k8s:worker"]},
        {**droplet, "networks": {"v4": []}},
    ]:
        with pytest.raises(t.Fatal):
            t.owned_droplet(bad, observed, "fixture", [])
    with pytest.raises(t.Fatal):
        t.owned_droplet(droplet, observed, "fixture", ["12"])


async def test_wait_retries_decode_error_but_not_fatal():
    callback = AsyncMock(side_effect=[json.JSONDecodeError("bad", "", 0), True])
    assert await t.wait_for("test", callback, 1, 0)
    with pytest.raises(t.Fatal):
        await t.wait_for("test", AsyncMock(side_effect=t.Fatal("stop")), 1, 0)


async def test_patch_retries_only_status_changes(opts, monkeypatch):
    cr = resource()
    fresh = copy.deepcopy(cr)
    fresh["metadata"]["resourceVersion"] = "11"
    fresh["status"]["phase"] = "Reconciling"
    call = AsyncMock(
        side_effect=[
            RuntimeError(
                "the server rejected our request due to an error in our request"
            ),
            fresh,
        ]
    )
    monkeypatch.setattr(t, "kubectl", call)
    monkeypatch.setattr(t, "get_resource", AsyncMock(return_value=fresh))
    monkeypatch.setattr(t.asyncio, "sleep", AsyncMock())
    assert (
        await t.patch(opts, cr, [{"op": "add", "path": "/spec/suspend", "value": True}])
        == fresh
    )
    assert json.loads(call.call_args.args[1][-1])[0]["value"] == "11"
    fresh["spec"]["config"]["profile"] = "changed"
    call.side_effect = RuntimeError(
        "the server rejected our request due to an error in our request"
    )
    with pytest.raises(t.Fatal):
        await t.patch(opts, cr, [])


async def test_rehearsal_timeout_leaves_suspended(opts, monkeypatch):
    cr = resource()
    suspended = copy.deepcopy(cr)
    suspended["metadata"]["generation"] = 3
    suspended["spec"]["suspend"] = True
    suspended["status"].update(phase="Suspended", observedGeneration=3)
    monkeypatch.setattr(t, "wait_ready", AsyncMock(return_value=cr))
    patch = AsyncMock(return_value=suspended)
    monkeypatch.setattr(t, "patch", patch)
    monkeypatch.setattr(t, "get_resource", AsyncMock(return_value=suspended))
    monkeypatch.setattr(
        t, "probe", AsyncMock(side_effect=RuntimeError("transport timed out"))
    )
    with pytest.raises(RuntimeError, match="resource left suspended"):
        await op.rehearse(opts)
    assert patch.await_count == 1
    evidence = json.loads(
        Path(
            opts["workdir"], opts["profile"], "evidence/backup-rehearsal.json"
        ).read_text()
    )
    assert not evidence["passed"]
    assert "resumeBlocked" in evidence


async def test_rehearsal_known_failure_resumes(opts, monkeypatch):
    cr = resource()
    suspended = copy.deepcopy(cr)
    suspended["metadata"]["generation"] = 3
    suspended["spec"]["suspend"] = True
    suspended["status"].update(phase="Suspended", observedGeneration=3)
    monkeypatch.setattr(t, "wait_ready", AsyncMock(return_value=cr))
    patch = AsyncMock(return_value=suspended)
    monkeypatch.setattr(t, "patch", patch)
    monkeypatch.setattr(t, "get_resource", AsyncMock(return_value=suspended))
    monkeypatch.setattr(t, "probe", AsyncMock(return_value={"rehearsalPassed": False}))
    with pytest.raises(RuntimeError, match="Backup rehearsal failed"):
        await op.rehearse(opts)
    assert patch.await_count == 2 and patch.call_args.args[2][0]["value"] is False


async def test_delete_waits_for_finalizer(opts, monkeypatch):
    cr = resource()
    cr["spec"]["deletionPolicy"] = "Destroy"
    monkeypatch.setattr(t, "get_resource", AsyncMock(return_value=cr))
    calls = []

    async def kubectl(_o, args, **_kw):
        calls.append(args)
        return {"items": []}

    monkeypatch.setattr(t, "kubectl", kubectl)

    async def wait(*args):
        raise RuntimeError("finalizer blocked")

    monkeypatch.setattr(t, "wait_for", wait)
    with pytest.raises(RuntimeError):
        await w.delete_step(opts)
    assert not any(args[:2] == ["delete", "namespace"] for args in calls)


async def test_unreadable_state_never_recreates():
    with pytest.raises(RuntimeError, match="could not be read"):
        await a.observe(
            {"profile": "fixture"},
            {"inspect": AsyncMock(return_value={"status": "error"})},
        )


async def test_absent_droplet_still_exists_during_delete():
    deps = {
        "inspect": AsyncMock(
            return_value={
                "status": "present",
                "cluster": {"nodes": [{"provider_id": "12"}]},
            }
        ),
        "provider_get": AsyncMock(return_value=None),
    }
    assert await a.observe({"profile": "fixture", "blue/event": "delete"}, deps) == {
        "exists": True,
        "matches": False,
        "ready": False,
    }
    assert await a.observe({"profile": "fixture"}, deps) == {
        "exists": False,
        "matches": False,
        "ready": False,
    }


def test_secret_mask_permissions_and_retention(tmp_path, monkeypatch):
    monkeypatch.setenv("COLORS_WORKDIR", str(tmp_path))
    config = {"profile": "fixture"}
    for i in range(23):
        a.retain_failure(
            config,
            {"blue/step": str(i), "blue/err": "long-secret secret"},
            {"COLORS_PAR_A": "secret", "COLORS_PAR_B": "long-secret"},
        )
    paths = list((tmp_path / "fixture/failures").glob("*.log"))
    assert len(paths) == 20
    assert all(
        stat.S_IMODE(x.stat().st_mode) == 0o600 and "secret" not in x.read_text()
        for x in paths
    )
    assert stat.S_IMODE(paths[0].parent.stat().st_mode) == 0o700


def test_probe_tokens_and_environment():
    with pytest.raises(ValueError):
        p.safe_token("x; touch /tmp/oops")
    with pytest.raises(RuntimeError):
        a.check_environment(
            {**{k: "secret" for k in t.CREDENTIALS}, "COLORS_PAR_PROFILE": "other"}
        )


@pytest.mark.parametrize("body", [b"", b"null", b"false", b"0", b"{}", b"[]"])
async def test_provider_empty_success_never_means_absent(monkeypatch, body):
    from unittest.mock import MagicMock

    response = MagicMock()
    response.__enter__.return_value.read.return_value = body
    monkeypatch.setattr(t.urllib.request, "urlopen", MagicMock(return_value=response))
    with pytest.raises(RuntimeError, match="absence requires HTTP 404"):
        await t.digitalocean("test-token", "get", "droplets/12")


async def test_provider_absence_requires_404_and_delete_allows_empty(monkeypatch):
    from unittest.mock import MagicMock

    monkeypatch.setattr(
        t.urllib.request,
        "urlopen",
        MagicMock(
            side_effect=t.urllib.error.HTTPError("url", 404, "missing", {}, None)
        ),
    )
    assert await t.digitalocean("test-token", "get", "droplets/12") is None
    response = MagicMock()
    response.__enter__.return_value.read.return_value = b""
    monkeypatch.setattr(t.urllib.request, "urlopen", MagicMock(return_value=response))
    assert await t.digitalocean("test-token", "delete", "droplets/12") is None
    response.__enter__.return_value.read.return_value = b'{"droplet":{"id":12}}'
    assert await t.digitalocean("test-token", "get", "droplets/12") == {
        "droplet": {"id": 12}
    }
