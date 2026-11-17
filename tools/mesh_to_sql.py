#!/usr/bin/env python3
"""mesh_to_sql.py — turn a human- or LLM-written mesh.json into GPUB setup commands.

A mesh.json describes your machines, the GPUs/accelerators they host (as resources, optionally from a hardware preset),
and groups (a duo, a quad, a Mac pair over Thunderbolt, two NVLinked GPUs) that are booked as one unit. This tool
VALIDATES it and prints:
  1. the `coordinator.py register` commands (one per machine; each prints a token once — give it to that machine), and
  2. SQL that inserts the resources and groups (idempotent: ON CONFLICT DO NOTHING).
It never connects to a database or the network. Standard library only.

    python tools/mesh_to_sql.py examples/mesh.example.json            # print commands + SQL
    python tools/mesh_to_sql.py mesh.json --sql-out mesh.sql          # write the SQL to a file
    python tools/mesh_to_sql.py mesh.json --check                     # validate only (exit 1 on errors)
    python tools/mesh_to_sql.py --presets                             # list hardware presets and link types
Schema: docs/describe-your-mesh.md
"""
from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path

PRESETS_FILE = Path(__file__).resolve().parent.parent / "examples" / "presets.json"
NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


def load_presets(path: Path = PRESETS_FILE) -> tuple[dict, dict]:
    p = json.loads(path.read_text())
    return {x["id"]: x for x in p["presets"]}, {x["id"]: x for x in p["links"]}


def resolve(mesh: dict, presets: dict, links: dict) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    """Return (machines, resources, groups, errors). Resources get memory_gb/budget_gb/model filled from presets."""
    errs: list[str] = []
    machines = mesh.get("machines") or []
    names = [m.get("name") for m in machines]
    for m in machines:
        if not NAME.match(str(m.get("name", ""))):
            errs.append(f"machine name {m.get('name')!r}: use lowercase letters, digits and dashes")
    for n in {n for n in names if names.count(n) > 1}:
        errs.append(f"machine {n!r} is listed twice")
    res_out = []
    for r in mesh.get("resources") or []:
        rid = r.get("id")
        if not NAME.match(str(rid or "")):
            errs.append(f"resource id {rid!r}: use lowercase letters, digits and dashes"); continue
        if r.get("machine") not in names:
            errs.append(f"resource {rid}: machine {r.get('machine')!r} is not in machines[]")
        pr = presets.get(r.get("preset") or "custom")
        if pr is None:
            errs.append(f"resource {rid}: unknown preset {r.get('preset')!r} (see --presets)"); continue
        mem = r.get("memory_gb", pr.get("memory_gb"))
        if mem is None:
            errs.append(f"resource {rid}: preset {pr['id']} needs memory_gb (how much memory this device has)"); continue
        budget = r.get("budget_gb", pr.get("budget_gb") if r.get("memory_gb") is None else None)
        if budget is None:
            budget = round(float(mem) * pr.get("budget_fraction", 0.9))
        if not (0 < float(budget) <= float(mem)):
            errs.append(f"resource {rid}: budget_gb {budget} must be > 0 and <= memory_gb {mem}")
        model = r.get("model") or (pr["label"] if pr.get("memory_gb") is not None else
                                   f"{pr.get('name', pr['label'])} ({mem} GB{' unified' if pr['memory'] == 'unified' else ''})")
        res_out.append({"id": rid, "machine": r.get("machine"), "model": model, "memory_gb": mem,
                        "budget_gb": budget, "lockfile": r.get("lockfile"), "notes": r.get("notes")})
    rids = [r["id"] for r in res_out]
    for n in {n for n in rids if rids.count(n) > 1}:
        errs.append(f"resource {n!r} is listed twice")
    groups_out = []
    for g in mesh.get("groups") or []:
        gid, mem = g.get("id"), g.get("members") or []
        if not NAME.match(str(gid or "")):
            errs.append(f"group id {gid!r}: use lowercase letters, digits and dashes"); continue
        if gid in rids:
            errs.append(f"group {gid}: id clashes with a resource id")
        if len(mem) < 2 or len(set(mem)) != len(mem):
            errs.append(f"group {gid}: needs 2+ distinct members")
        for m in mem:
            if m not in rids:
                errs.append(f"group {gid}: member {m!r} is not a resource")
        link = g.get("link", "none")
        if link not in links:
            errs.append(f"group {gid}: unknown link {link!r} (see --presets)")
        driver = g.get("driver") or (mem[0] if mem else None)
        if driver not in mem:
            errs.append(f"group {gid}: driver {driver!r} must be one of its members")
        by = {r["id"]: r for r in res_out}
        cap = sum(float(by[m]["budget_gb"]) for m in mem if m in by)
        groups_out.append({"id": gid, "members": mem, "link": link, "driver": driver,
                           "machine": by[driver]["machine"] if driver in by else None, "budget_gb": cap,
                           "label": f"{len(mem)}-member group over {links.get(link, {}).get('label', link)}"})
    gids = [g["id"] for g in groups_out]
    for n in {n for n in gids if gids.count(n) > 1}:
        errs.append(f"group {n!r} is listed twice")
    return machines, res_out, groups_out, errs


def q(v) -> str:
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def to_sql(resources: list[dict], groups: list[dict]) -> str:
    out = ["-- generated by tools/mesh_to_sql.py — resources first (a group's members must exist), then groups",
           "BEGIN;"]
    for r in resources:
        note = r["notes"] or "memory %s GB" % r["memory_gb"]
        out.append("INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, notes) VALUES "
                   f"({q(r['id'])}, {q(r['machine'])}, {q(r['model'])}, {r['budget_gb']}, {q(r['lockfile'])}, "
                   f"{q(note)}) ON CONFLICT (resource_id) DO NOTHING;")
    for g in groups:
        arr = "ARRAY[" + ",".join(q(m) for m in g["members"]) + "]"
        gnote = "driver %s; booked as one unit, never preempts" % g["driver"]
        out.append("INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, members, notes) VALUES "
                   f"({q(g['id'])}, {q(g['machine'])}, {q(g['label'])}, {g['budget_gb']}, NULL, {arr}, "
                   f"{q(gnote)}) ON CONFLICT (resource_id) DO NOTHING;")
    out.append("COMMIT;")
    return "\n".join(out) + "\n"


def register_cmds(machines: list[dict]) -> str:
    lines = ["# register each machine (prints its token ONCE; put it in that machine's ~/.config/gpub/config.json)"]
    for m in machines:
        ip = f" --ip {m['ip']}" if m.get("ip") else ""
        os_ = f" --os {m['os']}" if m.get("os") else ""
        lines.append(f"python gpub/coordinator.py --db gpub register {m['name']}{ip}{os_}")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("mesh", nargs="?"); ap.add_argument("--sql-out"); ap.add_argument("--check", action="store_true")
    ap.add_argument("--presets", action="store_true"); ap.add_argument("--presets-file", default=str(PRESETS_FILE))
    a = ap.parse_args()
    presets, links = load_presets(Path(a.presets_file))
    if a.presets:
        for p in presets.values():
            mem = f"{p['memory_gb']} GB" if p.get("memory_gb") else "memory_gb required"
            bud = f"budget {p['budget_gb']} GB" if p.get("budget_gb") else f"budget {int(p.get('budget_fraction', .9) * 100)}% of memory"
            print(f"  {p['id']:<14} {p['label']}  [{mem}; {bud}; telemetry: {p['telemetry']}]")
        print("links: " + ", ".join(links))
        return 0
    if not a.mesh:
        ap.error("give a mesh.json path (or --presets)")
    mesh = json.loads(Path(a.mesh).read_text())
    machines, resources, groups, errs = resolve(mesh, presets, links)
    if errs:
        print("mesh.json has problems:\n  - " + "\n  - ".join(errs), file=sys.stderr)
        return 1
    if a.check:
        print(f"ok: {len(machines)} machines, {len(resources)} resources, {len(groups)} groups"); return 0
    sql = to_sql(resources, groups)
    print(register_cmds(machines))
    if a.sql_out:
        Path(a.sql_out).write_text(sql); print(f"# SQL written to {a.sql_out}; load with: psql gpub -f {a.sql_out}")
    else:
        print(sql)
    return 0


if __name__ == "__main__":
    sys.exit(main())
