# Describe your mesh (by hand, in the browser, or with an LLM)

Most homelabs are bought one machine at a time: a gaming PC with an RTX 3090, a MacBook you sometimes dock, a Mac Studio,
a pair of GB10 boxes, an old rig with two NVLinked cards. GPUB needs to know three things about that pile of hardware:

1. **Machines**: every computer that books GPUs or hosts them.
2. **Resources**: every GPU or accelerator that can be booked, with a memory **budget**. Two GPUs in one box are two resources.
3. **Groups**: sets of resources that are booked **together as one unit**. Examples: a Mac pair over Thunderbolt, two NVLinked GPUs, a GB10 duo, a 4-box quad.

You write this down once as a small `mesh.json`. Then `tools/mesh_to_sql.py` (or the dashboard's **Build my mesh** tab)
checks it and gives you the exact setup commands.

There are three ways to make the file:

- **In the browser.** Open `viz/gpubdash.html?build`, add machines, GPUs and groups from the hardware presets, then click
  *Export mesh.json* or *Show setup commands*. Nothing leaves your browser.
- **By hand.** Copy `examples/mesh.example.json` and edit it.
- **With an LLM.** Paste the prompt below into the assistant you use, answer its questions, and paste the JSON it returns
  into *Import mesh.json*, or save it and run `python tools/mesh_to_sql.py mesh.json --check`.

## The format

```json
{
  "mesh": "my-homelab",
  "machines": [
    {"name": "headnode", "os": "linux",  "ip": "192.0.2.10", "role": "runs the coordinator"},
    {"name": "oldrig",   "os": "linux",  "ip": "192.0.2.12"},
    {"name": "macbook",  "os": "macos",  "ip": "192.0.2.32"}
  ],
  "resources": [
    {"id": "oldrig-3090-0", "machine": "oldrig",  "preset": "rtx-3090"},
    {"id": "oldrig-3090-1", "machine": "oldrig",  "preset": "rtx-3090"},
    {"id": "macbook-gpu",   "machine": "macbook", "preset": "apple-silicon", "memory_gb": 64, "budget_gb": 44,
     "notes": "laptop: only when docked and on mains"}
  ],
  "groups": [
    {"id": "oldrig-nvlink", "members": ["oldrig-3090-0", "oldrig-3090-1"], "link": "nvlink"}
  ]
}
```

| Field | Required | Meaning |
|---|---|---|
| `machines[].name` | yes | Lowercase letters, digits and dashes. It becomes the machine's identity. |
| `machines[].os` | no | `linux`, `linux-wsl2`, `macos` or `windows`. Informational. |
| `machines[].ip` | no, but recommended | The machine's **private** LAN or VPN address. GPUB only accepts a machine's token from this address. Never use a public IP. |
| `machines[].role` | no | Free text. `"runs the coordinator"` marks the head node in the dashboard. |
| `resources[].id` | yes | A unique name for one GPU or accelerator, for example `oldrig-3090-0`. |
| `resources[].machine` | yes | Which machine it is in. Must match a `machines[].name`. |
| `resources[].preset` | no (default `custom`) | A hardware preset from `examples/presets.json` (list them with `python tools/mesh_to_sql.py --presets`). |
| `resources[].memory_gb` | only for `apple-silicon` and `custom` | How much memory the device has. For a Mac, its total RAM, which is unified memory. |
| `resources[].budget_gb` | no | How much of that memory bookings may use. Defaults come from the preset; on a Mac the default is 75% of RAM. |
| `resources[].lockfile` | no | Path of an existing lockfile used by older tools on that box (see the external lockfile wrap in the README). |
| `resources[].notes` | no | Free text. |
| `groups[].id` | yes | A unique name, for example `mac-pair` or `gb10-quad`. |
| `groups[].members` | yes | 2 or more resource ids, all booked together and held exclusive. |
| `groups[].link` | no (default `none`) | `qsfp`, `thunderbolt`, `nvlink`, `ethernet`, `infiniband` or `none`. Informational: GPUB does not configure the link. |
| `groups[].driver` | no (default: first member) | The member whose host agent starts a group booking's job. |

The hardware presets are `rtx-3060-12`, `rtx-3090`, `rtx-4090`, `rtx-5090`, `rtx-a6000`, `rtx-pro-6000`, `gb10` (DGX Spark-class),
`apple-silicon` and `custom`. Budgets are policy, not measurements: leave headroom for the OS and driver. Edit
`examples/presets.json` to add your own presets.

## Prompt for an LLM

Copy everything in the box into your assistant, then answer its questions.

```text
You are helping me describe my home GPU lab for GPUB, a GPU booking tool. Ask me short questions, one topic at a
time, until you know:
1. every computer I have that runs AI jobs or would book GPUs (name, operating system: linux / linux-wsl2 / macos /
   windows, and its PRIVATE LAN or VPN IP if I know it; never a public IP);
2. every GPU or accelerator in each computer. Two cards in one box are two entries. Map each to one of these presets:
   rtx-3060-12, rtx-3090, rtx-4090, rtx-5090, rtx-a6000, rtx-pro-6000, gb10 (NVIDIA DGX Spark / GB10 class),
   apple-silicon (ask me the Mac's total RAM in GB), custom (ask me its memory in GB);
3. which devices are physically linked and should be booked TOGETHER as one unit (for example two Macs over
   Thunderbolt, two NVLinked cards, GB10 boxes connected by QSFP cables), and the link type:
   qsfp, thunderbolt, nvlink, ethernet, infiniband or none;
4. which machine will run the GPUB coordinator.
Rules: names use only lowercase letters, digits and dashes. A resource id is usually <machine>-<gpu>, e.g.
"oldrig-3090-0". Do not invent hardware I did not mention. If I am unsure about memory, say so instead of guessing.
For a laptop, add a note like "only when docked and on mains".
When you have everything, output ONLY one JSON object in exactly this shape, with no comments:
{"mesh": "...", "machines": [{"name": "...", "os": "...", "ip": "...", "role": "..."}],
 "resources": [{"id": "...", "machine": "...", "preset": "...", "memory_gb": 0, "budget_gb": 0, "notes": "..."}],
 "groups": [{"id": "...", "members": ["...", "..."], "link": "...", "driver": "..."}]}
Omit optional fields you don't know (ip, role, memory_gb for fixed presets, budget_gb, notes, driver).
```

Then check the result. LLMs make mistakes, and the checker catches the common ones: unknown machines, duplicate
names, a budget larger than memory, groups with fewer than two members.

```bash
python tools/mesh_to_sql.py mesh.json --check           # ok: 7 machines, 7 resources, 3 groups
python tools/mesh_to_sql.py mesh.json --sql-out mesh.sql  # prints the register commands, writes the SQL
```

## What happens next

1. Run each printed `coordinator.py ... register <machine>` command on the head node. Each one prints a token **once**.
   Put that token in the matching machine's `~/.config/gpub/config.json` (see `examples/config.example.json`).
2. Load the resources and groups: `psql gpub -f mesh.sql`. The file is idempotent, so running it twice is harmless.
3. Start an agent on each machine that hosts a resource (see `deploy/`). Do not run an agent for a group resource:
   the driver member's agent runs group jobs.
4. Open `http://<head node>:8766/dash` to see the mesh live.

## Honest limits

- **Telemetry.** The agent reads `nvidia-smi`. Apple Silicon and non-NVIDIA devices are bookable, and the agent still
  starts jobs and heartbeats on them, but GPUB shows no GPU telemetry for them.
- **Multi-GPU boxes.** Each GPU is its own bookable resource, but the agent's telemetry reads the machine as a whole,
  not per GPU. Pinning a job to a specific GPU (`CUDA_VISIBLE_DEVICES`) is up to the job.
- **Links are descriptive.** GPUB does not discover, configure or test QSFP, Thunderbolt or NVLink. It only makes sure a group's members are booked together.
