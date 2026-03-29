from __future__ import annotations

import asyncio
import subprocess

from fastapi import FastAPI, HTTPException

app = FastAPI(title="Chaos Controller")

NODES = {
    "pg-master": {"container": "chaos-pg-master", "port": 15432},
    "pg-replica-1": {"container": "chaos-pg-replica-1", "port": 15433},
    "pg-replica-2": {"container": "chaos-pg-replica-2", "port": 15434},
}


def _docker_exec(container: str, cmd: list[str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["sudo", "docker", "exec", container, *cmd],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(
            status_code=504, detail=f"docker exec timed out on {container}"
        ) from exc


def _docker_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["sudo", *cmd],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(
            status_code=504, detail=f"command timed out: {' '.join(cmd)}"
        ) from exc


def _get_container(node: str) -> str:
    if node not in NODES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown node: {node}. Valid: {list(NODES)}",
        )
    return NODES[node]["container"]


def _is_container_running(container: str) -> bool:
    try:
        result = subprocess.run(
            ["sudo", "docker", "inspect", "-f", "{{.State.Running}}", container],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() == "true"
    except subprocess.TimeoutExpired:
        return False


def _get_node_status(node: str, info: dict) -> tuple[str, dict]:
    container = info["container"]
    running = _is_container_running(container)
    if not running:
        return node, {"running": False, "role": None}

    try:
        result = _docker_exec(
            container,
            ["psql", "-U", "postgres", "-h", "127.0.0.1", "-d", "testdb", "-tA",
             "-c", "SELECT pg_is_in_recovery()"],
        )
    except HTTPException:
        return node, {"running": True, "role": "unknown", "error": "timed out"}

    if result.returncode != 0:
        return node, {
            "running": True,
            "role": "unknown",
            "error": result.stderr.strip(),
        }

    is_recovery = result.stdout.strip() == "t"
    role = "replica" if is_recovery else "master"
    return node, {"running": True, "role": role}


@app.get("/status")
async def status():
    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(None, _get_node_status, node, info)
        for node, info in NODES.items()
    ]
    results_list = await asyncio.gather(*tasks)
    return dict(results_list)


@app.post("/reset")
async def reset():
    loop = asyncio.get_running_loop()
    actions = []
    for node, info in NODES.items():
        container = info["container"]
        running = await loop.run_in_executor(None, _is_container_running, container)
        if not running:
            result = await loop.run_in_executor(
                None, _docker_cmd, ["docker", "start", container],
            )
            if result.returncode != 0:
                raise HTTPException(
                    status_code=500,
                    detail=f"failed to start {node}: {result.stderr.strip()}",
                )
            actions.append(f"restarted {node}")
        else:
            result = await loop.run_in_executor(
                None,
                _docker_exec,
                container,
                ["iptables", "-F", "INPUT"],
            )
            if result.returncode != 0:
                raise HTTPException(
                    status_code=500,
                    detail=f"failed to flush iptables on {node}: {result.stderr.strip()}",
                )
            actions.append(f"flushed iptables on {node}")
    return {"actions": actions}


@app.post("/freeze/{node}")
async def freeze(node: str):
    container = _get_container(node)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        _docker_exec,
        container,
        ["sh", "-c",
         "iptables -C INPUT -p tcp --dport 5432 -j DROP 2>/dev/null || "
         "iptables -A INPUT -p tcp --dport 5432 -j DROP"],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "freeze", "node": node}


@app.post("/unfreeze/{node}")
async def unfreeze(node: str):
    container = _get_container(node)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        _docker_exec,
        container,
        ["sh", "-c",
         "iptables -C INPUT -p tcp --dport 5432 -j DROP 2>/dev/null && "
         "iptables -D INPUT -p tcp --dport 5432 -j DROP"],
    )
    if result.returncode != 0 and result.stderr.strip():
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "unfreeze", "node": node}


@app.post("/promote/{node}")
async def promote(node: str):
    container = _get_container(node)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        _docker_exec,
        container,
        ["psql", "-U", "postgres", "-h", "127.0.0.1", "-c", "SELECT pg_promote();"],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "promote", "node": node}


@app.post("/kill/{node}")
async def kill_node(node: str):
    container = _get_container(node)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None, _docker_cmd, ["docker", "stop", container],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "kill", "node": node}


@app.post("/restart/{node}")
async def restart_node(node: str):
    container = _get_container(node)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None, _docker_cmd, ["docker", "start", container],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "restart", "node": node}
