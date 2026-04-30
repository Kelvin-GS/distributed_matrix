# Distributed Matrix System

This is a  peer-collaborative platform that distributes **matrix multiplication** across any collection of devices on a local network say laptops, desktops, and mobile phones included. Devices on the same WiFi divide the work, compute their portions independently, and the system stays running even when individual nodes drop out mid-job.

---

## The Idea

The goal here was to build something that works with any connected device say, Windows laptops, a MacBook, an Android phone, and an iPhone. All devices contributing real CPU work to the same computation, with the system smart enough to recover automatically when any of them disconnects.

Matrix multiplication workload partitions cleanly: split Matrix A into row-blocks, send each block to a different worker along with the full Matrix B, collect the partial results, and assemble the final matrix. Workers never need to talk to each other only to the coordinator. That property makes it an ideal fit for a heterogeneous, unreliable network.

The interesting engineering is not the multiplication itself. It is everything around it: automatic node discovery, fault-tolerant state persistence, coordinator election on failure, and getting a phone browser to do genuine matrix arithmetic and prove it.

---

## What the System Does

- Accepts two matrices of any size (2×2 up to 200×200 and beyond) submitted from any device on the network
- Partitions the job into row-blocks and distributes them across all connected nodes simultaneously
- Python nodes compute their blocks natively; phones and tablets compute using a JavaScript Web Worker running in the browser — same algorithm, different runtime, same accountability
- Every node reports back compute time, MFLOPS, total operations, and device information as verifiable proof of local computation
- All job state is written to a SQLite database before computation begins and replicated in real time to three backup nodes
- If the coordinating node disconnects mid-job, the remaining nodes elect a new coordinator automatically: it reads the persisted state and resumes only the pending blocks; **completed work is never repeated**
- Results persist for two hours after job completion; a client that disconnects and reconnects receives its result immediately on return
- Multiple jobs from different users run simultaneously without interfering with each other — each job manages its own coordinator independently

---

## Architecture Overview

```
┌──────────────────────────── LAN (MiFi / WiFi) ──────────────────────────────┐
│                                                                              │
│   LAPTOP / DESKTOP                LAPTOP / DESKTOP           PHONE          │
│  ┌──────────────────┐            ┌──────────────────┐    ┌─────────────┐    │
│  │  Python Server   │◄──────────►│  Python Server   │◄──►│  Browser    │    │
│  │  FastAPI :8080   │            │  FastAPI :8080   │    │  PWA :8080  │    │
│  │                  │            │                  │    │             │    │
│  │  Coordinator     │            │  Worker          │    │  JS Worker  │    │
│  │  SQLite (primary)│            │  SQLite (backup) │    │  (compute)  │    │
│  └──────────────────┘            └──────────────────┘    └─────────────┘    │
│           │                             │                       │           │
│           └─────────────────────────────┴──────── mDNS ─────────┘           │
│                          (zero-config automatic discovery)                   │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Node discovery** is handled by mDNS (Zeroconf). When a node starts, it announces itself on the LAN. Every other node on the subnet sees the announcement within ~500ms. No IP addresses are configured manually anywhere.

**The coordinator** is whoever submitted the job. It holds no private state, everything is in SQLite, so if it dies, any other node can take over by reading the database. The new coordinator is chosen by a ***Bully Election*** among the remaining nodes: the node with the highest ID that responds wins.

**Workers** receive their assigned row-block of Matrix A and the full Matrix B over WebSocket or HTTP, compute the partial product, and return the result with metrics. Python workers run the computation in an async executor (non-blocking). Browser workers run it in a Web Worker thread (also non-blocking: the UI staying live during computation).

**State replication** happens on every write. Each time the coordinator updates the database say, assigning a block, recording a result, marking a job complete: it sends the same operation to three backup nodes, which apply it to their local SQLite copies. Failover is just a matter of reading from the nearest copy.

---

## Prerequisites

### Computers (Python nodes)

| Requirement | Version                               | Notes                            |
| ----------- | ------------------------------------- | -------------------------------- |
| Python      | 3.10 or higher                        | Verify with `python --version` |
| pip         | Any recent version                    | Included with Python             |
| OS          | Windows 10+, Ubuntu 20.04+, macOS 12+ | All supported                    |
| Network     | Same LAN as other nodes               | MiFi hotspot works fine          |

### Phones and Tablets

| Requirement        | Notes                                                                                              |
| ------------------ | -------------------------------------------------------------------------------------------------- |
| Any modern browser | Chrome, Firefox, Safari, Microsoft Edge, Opera, Brave                                              |
| Same WiFi network  | Must be on the same subnet as**at least one Python node,** even a personal MIFI, or hotspot |
| Nothing to install | Open the browser, navigate to any node's IP, that is all                                          |

### Network

- All devices on the **same subnet** (same router or MiFi hotspot)
- Port **8080** reachable on each Python node (check local firewall settings)
- mDNS / multicast traffic not blocked by the router: standard home and mobile routers are fine; some enterprise/university networks block multicast, in which case nodes can be registered manually via `POST /nodes/register`
- Internet access is **not required** at any point

## Installation

### 1. Clone the repository

bash

```bash
git clone https://github.com/Kelvin-GS/distributed_matrix.git
cd distributed_matrix
```

### 2. Install dependencies

bash

```bash
pip install -r requirements.txt
```

| Package               | Purpose                                          |
| --------------------- | ------------------------------------------------ |
| `fastapi`           | HTTP REST API and WebSocket server               |
| `uvicorn[standard]` | ASGI server that runs FastAPI                    |
| `aiohttp`           | Async HTTP client for node-to-node communication |
| `zeroconf`          | mDNS / Zeroconf for automatic node discovery     |
| `websockets`        | WebSocket protocol support                       |

### 3. Start the node

#### Ubuntu / Debian

Make sure Python 3.10+ is installed. On older Ubuntu versions (20.04 and below) the default Python may be 3.8 — upgrade it first:

bash

```bash
sudoapt update
sudoaptinstall python3.11 python3.11-pip python3.11-venv -y
```

Then set up and run:

bash

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

If port 8080 is blocked by UFW (Ubuntu's firewall), open it:

bash

```bash
sudo ufw allow 8080
```

mDNS on Ubuntu also requires Avahi running. Confirm it is active:

bash

```bash
sudo systemctl status avahi-daemon
```

If it is not running:

bash

```bash
sudoaptinstall avahi-daemon -y
sudo systemctl enable --now avahi-daemon
```

---

#### Windows 10 / 11

First confirm Python 3.10+ is installed by opening Command Prompt or PowerShell:

powershell

```powershell
python --version
```

If Python is not installed, download it from [python.org](https://www.python.org/downloads/). During installation, make sure to check  **"Add Python to PATH"** .

Then in Command Prompt or PowerShell:

powershell

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

**Firewall note:** Windows Defender Firewall will likely prompt you the first time the node starts, asking whether to allow Python on the network. Click **Allow Access** on both private and public networks. If the prompt never appeared and other nodes cannot reach this one, add the rule manually:

```
Control Panel → Windows Defender Firewall → Advanced Settings
→ Inbound Rules → New Rule → Port → TCP → 8080 → Allow the connection
```

**mDNS on Windows:** The `zeroconf` library uses Windows' built-in mDNS stack. No additional installation is required. If node discovery is not working, check that the **Bonjour** service is not disabled — it is installed by iTunes and some other Apple software and can conflict. If Bonjour is present and disabled, either enable it or uninstall it; the Python `zeroconf` library does not depend on it.

---

#### macOS 12+

macOS ships with Python 2.7 by default in older versions. Confirm you have 3.10+:

bash

```bash
python3 --version
```

If not, install via Homebrew:

bash

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install python@3.11
```

Then set up and run:

bash

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

**Firewall note:** macOS may show a prompt asking if you want to allow incoming connections to Python. Click  **Allow** . If the prompt does not appear and other nodes cannot reach this one, go to:

```
System Settings → Privacy & Security → Firewall → Firewall Options
```

Add Python to the allowed applications list.

mDNS works natively on macOS through the built-in Bonjour service — no additional setup required.

---

On a successful start on any platform you will see:

```
2026-04-28 17:10:21,183 [node] INFO — Loaded persistent node_id: e802fef3
2026-04-28 17:10:21,186 [storage] INFO — SQLite initialised at /home/kelvin/Desktop/distributed_matrix/matmul.db (WAL mode)
2026-04-28 17:10:21,187 [main] INFO — Starting distributed matrix node on port 8080...
2026-04-28 17:10:21,187 [node] INFO — Node e802fef3 starting on 192.168.0.108:8080
2026-04-28 17:10:21,647 [discovery] INFO — Registered mDNS service: e802fef3-a299-49b8-8953-84e422c9eac2._matmul._tcp.local. (port 8080)
2026-04-28 17:10:21,647 [discovery] INFO — Discovery started. Local IP: 192.168.0.108, port: 8080
2026-04-28 17:10:21,670 [node] INFO — Web UI at http://192.168.0.108:8080
```

The IP address shown is what other devices use to reach this node. Every device running the system is also serving the web interface on that address.

To run multiple nodes on the same machine (useful for local testing), open a separate terminal for each and specify different ports:

bash

```bash
python main.py --port 8080
python main.py --port 8081
python main.py --port 8082
```

---

## Adding Devices

### Adding another computer

Clone the repository, install dependencies following the steps for your OS above, then run:

bash

```bash
# Ubuntu / macOS
python3 main.py

# Windows
python main.py
```

The new node is discovered automatically within ~500ms. No configuration changes are needed on any existing node.

## Adding a phone or tablet

1. Connect the device to the same WiFi network
2. Open any Python node's IP address in the browser: `http://192.168.1.42:8080`
3. **The device registers itself as a worker node immediately**

That is all. The phone will receive block assignments, compute them in a background thread, and return results with timing metrics, exactly as a Python node does, just via the browser runtime instead.

---

## Usage

### Submitting a job

1. Open `http://[any-node-ip]:8080` on any device
2. Set the dimensions of Matrix A and Matrix B
3. Enter values manually or click **Fill Random**
4. For matrices larger than 15×15, the input grid is hidden — click a size preset (**50×50**, **100×100**) or set custom dimensions and the system generates random values automatically
5. Click **Run Distributed Multiply**

A job ID is assigned immediately. The result appears in the Result panel when all workers have returned their blocks. For large result matrices a **Download CSV** button appears in place of the inline display.

### Metrics panel

Every device shows a **My Node Contribution** panel. For phones and browsers this panel is the primary evidence of local computation:

| Metric         | Description                                                          |
| -------------- | -------------------------------------------------------------------- |
| Compute Time   | Milliseconds the device's CPU spent on the matrix multiplication     |
| MFLOPS         | Floating-point throughput:`(2 · rows · k · n) / time_ms / 1000` |
| Operations     | Total multiply-add pairs executed                                    |
| Rows Processed | Number of result rows this device computed                           |
| Device         | Browser user-agent string identifying the hardware                   |

The compute time is measured using `performance.now()` in browsers (microsecond resolution) and `time.perf_counter()` in Python. The operation count is deterministic from the block dimensions. Together these give a verifiable picture of what each device contributed.

### Result availability

Results are retained for two hours. If your device disconnects before the job completes, or before you have seen the result, reconnect to any node's web interface. The result is pushed to you immediately on reconnection. Results survive coordinator failures because they are written to SQLite, not held in memory.

---

## Project Structure

```
distributed-matrix-system/
│
├── main.py            # Entry point — run this to start a node
├── node.py            # Top-level orchestrator — wires all subsystems together
├── server.py          # FastAPI HTTP + WebSocket server
├── coordinator.py     # Stateless job coordinator — reads and writes SQLite only
├── worker.py          # Python compute engine — triple-loop matrix multiplication
├── storage.py         # SQLite persistence layer (WAL mode, async-safe)
├── election.py        # Bully election algorithm — per-job coordinator failover
├── discovery.py       # mDNS/Zeroconf node discovery
├── models.py          # Shared data structures and message factories
├── config.py          # All tunable constants
├── requirements.txt   # Python dependencies
│
└── web/
    ├── index.html     # Web interface — served by every node on port 8080
    ├── app.js         # Browser application logic and WebSocket client
    ├── worker.js      # JavaScript Web Worker for browser-side computation
    ├── style.css      # Responsive dark UI
    └── manifest.json  # PWA manifest
```

---

## Configuration

All tunable parameters are in `config.py`. The defaults are set for a 40–50 node LAN:

| Constant               | Default   | Description                                                     |
| ---------------------- | --------- | --------------------------------------------------------------- |
| `NODE_PORT`          | `8080`  | HTTP port each node listens on                                  |
| `HEARTBEAT_INTERVAL` | `1.0s`  | How often nodes send heartbeats to peers                        |
| `HEARTBEAT_TIMEOUT`  | `3.0s`  | Missed heartbeat window before a node is declared unreachable   |
| `BLOCK_TIMEOUT`      | `30.0s` | Time before an unacknowledged block is reassigned               |
| `ELECTION_TIMEOUT`   | `5.0s`  | Time to wait for OK responses before declaring election victory |
| `RESULT_TTL`         | `7200s` | How long results are retained after job completion              |
| `NUM_BACKUP_NODES`   | `3`     | Number of nodes that receive SQLite state replication           |
| `MAX_DIM`            | `500`   | Maximum matrix dimension the system will accept                 |

---

## Testing

### Local two-node test

```bash
# Terminal 1
python main.py --port 8080

# Terminal 2
python main.py --port 8081
```

Open `http://localhost:8080`, submit a job, and watch both terminals log block assignments and completions. This confirms node discovery, block distribution, and result assembly work correctly on a single machine.

### Phone compute verification

1. Start at least one Python node
2. Connect a phone to the same WiFi and open the node's IP in the browser
3. Submit a job from any device
4. Observe the phone's **My Node Contribution** panel: compute time and MFLOPS update as blocks are processed, confirming the phone's CPU did the work

### Coordinator failover test

1. Start three or more nodes on separate machines
2. Submit a large job (100×100 or 200×200) - the submitting node becomes coordinator
3. Kill the coordinator mid-job (`Ctrl+C`)
4. Observe the remaining nodes: election runs in ~4–5 seconds, the new coordinator logs which blocks it is resuming, the job completes

Expected log output after failover:

```
[election] INFO — [Election][f47ac10b] Starting — I am b92d1e44
[election] INFO — [Election][f47ac10b] I WON. Announcing.
[coordinator] INFO — Resuming job f47ac10b after election win
[coordinator] INFO — 12 pending blocks to reassign for job f47ac10b
[coordinator] INFO — Job f47ac10b COMPLETE in 1203.6ms
```

---

## Known Limitations

**B matrix broadcast** — The full Matrix B is sent to every worker. For 50 nodes and a 100×100 matrix this is roughly 4MB of total network traffic per job, which is fine in practice. At several hundred nodes it becomes a bottleneck. The proper fix is to column-partition B and use a ring pipeline (SUMMA algorithm), which is documented in the design document but not implemented in this version.

**MiFi as network infrastructure** — If the MiFi device itself fails, the LAN goes down entirely. That is a hardware concern, not something the software can work around. This is a project on Distributed Processing afterall. A managed network switch removes this dependency.

**Phone CPU utilisation** — Browser sandboxing prevents access to OS-level CPU percentage. The metrics panel uses `performance.now()` timing and deterministic operation counts instead, which are honest and verifiable.

**No authentication** — All nodes on the network are trusted. This is appropriate for a controlled private LAN. For broader deployment, mutual TLS between nodes is the correct addition.

**SQLite write serialisation** — SQLite processes one write at a time. At the scale this system targets (tens of nodes, sequential job submissions) this is not a bottleneck. Under very high concurrent write load, replacing the storage layer with a persistent Redis instance would be the right move.

---

## Where to Take It Next

The most impactful extensions, in rough priority order:

- **Smarter B distribution** — implement SUMMA-style column partitioning and a ring broadcast so the system scales cleanly past ~100 nodes
- **Hot standby coordinator** — maintain a pre-elected backup that already holds coordinator state, reducing election recovery time from ~5 seconds to near-zero
- **MessagePack serialisation** — replace JSON for matrix transfer with MessagePack, which reduces payload sizes by 30–50% with no loss of precision
- **Adaptive block sizing** — weight block assignments by each node's observed throughput (MFLOPS from previous blocks) rather than dividing rows equally

---

## Design Document

A full design document is included in the repository (`Project_Design.docx`). It covers every architectural decision made during development; the options considered, why each one was chosen or rejected, overhead analysis, scalability proofs with worked numerical examples, and the complete SQLite schema. If you want to understand the reasoning behind how the system is built, start there before reading the code.

---

## References

- Tanenbaum, A.S. & Van Steen, M. — *Distributed Systems: Principles and Paradigms* (3rd ed.)
- Garcia-Molina, H. (1982) — *Elections in a Distributed Computing System* — the original Bully Algorithm paper
- FastAPI documentation — https://fastapi.tiangolo.com
- Python Zeroconf — https://python-zeroconf.readthedocs.io
- MDN Web Docs: Web Workers API — https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API
- SQLite WAL Mode — https://www.sqlite.org/wal.html
