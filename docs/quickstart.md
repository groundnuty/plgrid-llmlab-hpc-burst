# Quickstart

This page takes you from a clone of the repository, or from the release archive, to one request through the front, on Athena. You start a control-plane host on your own machine. You tell it where a model may run (a Provider) and which model you want (a ModelDeployment). You then send an OpenAI-style request to the front, by the model's plain name. The framework submits and supervises the Slurm jobs.

The route is **Athena, in `host-dialed` mode**. Your host dials each job through the login node, so no compute node ever has to reach your machine. Helios is beta and waits for the GH200 nodes to return ([Helios](#helios-beta)).

The framework is alpha, and real runs are still being measured. Follow them in [issue #145](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/145). [Alpha's limits](#alphas-limits) at the end lists what to expect.

Read [HBEP-2](../hbep/0002-scope-and-user-story/README.md) first for what the framework is for. Each rule this page relies on lives in an HBEP, and the steps link there instead of repeating it.

Commands run from the repository root, or, on the release archive's path (step 2), from the directory you unpacked the archive in. `<user>`, `<account>`, `<grant>` and `<login-node>` stand for your own values. Nothing private belongs in a manifest you share.

## 1. What you need

- **An Athena grant** for GPU jobs: its account name, and a login on the Athena login node.
- **An ssh key** for that login node that works without a passphrase prompt. The transport runs ssh in batch mode ([HBEP-4](../hbep/0004-provider-interface/README.md), Transport). Log in by hand once, so that the login node's host key is in your `known_hosts`. The transport always checks it.
- **A host** for the control plane: a Linux machine that you use as a plain user, with no root and no system change ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Single-user host). It runs a user-level k3s (about 400 MiB of memory), the controllers, the front and frps (a tunnel server). They listen on loopback only, on a block of 13 ports starting at 29220 (`hpcburst up -h`, `--port-base`). Those ports must be free.
- **Tools:** `curl`, `openssl` and an OpenSSH client. To build from source you also need Go 1.26 or later ([development.md](development.md) lists the pins). The frps download in step 2 is pinned for Linux on x86_64. On another architecture, supply your own frps with `hpcburst up --frps-bin`, which checks it against the launcher's pin.

You need no gateway, no port forward and no restricted key. In `host-dialed` mode the Provider's ssh credentials are all that the host asks for. The other two modes, `direct` and `ssh`, need a route that you provide ([Alpha's limits](#alphas-limits)).

## 2. Build and fetch the binaries

Two directories matter. The state directory holds the host's records, its kubeconfig and the controller's agent builds. The binary directory holds the pinned k3s and frps. Set both once, in the shell you use for the rest of this page. The two lines below give the launcher's own defaults, and follow `XDG_STATE_HOME`, `XDG_CACHE_HOME` and `HPCBURST_BIN_DIR` when you have set them:

```
STATE=${XDG_STATE_HOME:-$HOME/.local/state}/hpcburst
BIN=${HPCBURST_BIN_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst/k3s}
```

Every command below passes `"$STATE"` or `"$BIN"` on purpose, to the launcher as `--state-dir` and `--bin-dir` and to the scripts as their directory argument. So the build step, the launcher and `kubectl` cannot end up in different places. Set both again in a new shell. To use other directories, give the two lines other values.

Choose one of the two ways to get the binaries.

### Build from source

```
mkdir -p bin
go build -o bin/ ./cmd/hpcburst ./cmd/hpcburst-controller ./cmd/hpcburst-front
hack/fetch-k3s.sh "$BIN"
hack/fetch-frp.sh "$BIN/frp-0.71.1"
```

- `hpcburst up` looks for `hpcburst-controller` and `hpcburst-front` beside itself, which is why the three share `bin/`.
- `hack/fetch-k3s.sh` puts the pinned k3s releases under `"$BIN"/k3s-<version>/` and checks their sha256 against the launcher's pins ([HBEP-15](../hbep/0015-dependencies-and-provenance/README.md)).
- `hack/fetch-frp.sh DIR` places the pinned `frps` and `frpc` in `DIR`. `up` looks for `frps` in `frp-0.71.1/` under the binary directory.

The jobs also need an allocation agent and `frpc`, built for the CPU architecture of the partition. The Provider's controller stages them from `agent-builds/` in the state directory ([HBEP-4](../hbep/0004-provider-interface/README.md), Submission). Athena's `plgrid-gpu-a100` is `x86_64`, so build that one:

```
install -d -m 700 "$STATE" "$STATE/agent-builds"
hack/fetch-frp.sh --agent-builds "$STATE/agent-builds"
hack/build-agents.sh "$STATE/agent-builds" x86_64
```

The controller records the agent build's sha256 with every job it submits. The script pins the Go toolchain, the flags and the environment, so the same source gives the same digest on any machine. It prints each digest with the Go version beside it.

### Release archive (once v0.1.0-alpha.1 is published)

Once release v0.1.0-alpha.1 is published, you can unpack its archive instead of building. Until then this path does not exist, and you build from source. The archive is for Linux on x86_64 and replaces the two blocks above.

You need no clone of the repository. Besides the binaries, the archive carries the `config/crd`, `profiles/` and `examples/` that the later steps read ([HBEP-15](../hbep/0015-dependencies-and-provenance/README.md), Releases). Make an empty directory, download `hpcburst-v0.1.0-alpha.1-linux-amd64.tar.gz` and `SHA256SUMS` from the release page into it, and run the rest of this page from there. Check the archive, unpack it in place, check every file it unpacked, and put k3s, frps and the agent builds where the launcher and the controller read them:

```
sha256sum -c --ignore-missing SHA256SUMS
tar -xzf hpcburst-v0.1.0-alpha.1-linux-amd64.tar.gz
sha256sum -c --quiet SHA256SUMS
mkdir -p "$BIN"
cp -r bin/k3s-1.36.5 bin/frp-0.71.1 "$BIN"/
install -d -m 700 "$STATE" "$STATE/agent-builds"
cp agent-builds/* "$STATE/agent-builds/"
```

The archive's `bin/` already holds the three host binaries side by side, as `up` needs them, and its `agent-builds/` holds the agent and `frpc` for both `x86_64` and `aarch64`. From here on the steps are the same for both ways.

## 3. Write your credentials files

The API never holds a credential, only the path of a file. Each file is mode 600 and owned by the user who runs the controllers. A file that others can read is refused, and the Provider shows `CredentialsInvalid` ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Credentials). Create each file empty with the right mode, then edit it, so that a secret never appears on a command line:

```
install -d -m 700 ~/.config/hpcburst ~/.config/hpcburst/credentials
install -m 600 /dev/null ~/.config/hpcburst/credentials/athena-a100.ssh
```

**The Athena file** is an ssh client configuration with at most one `Host` line. It takes `HostName`, `User` and `IdentityFile`, and optionally `Port`, `UserKnownHostsFile`, `StrictHostKeyChecking yes` and `IdentitiesOnly yes`. Any other keyword is refused, `ProxyJump` and the forwards included ([HBEP-4](../hbep/0004-provider-interface/README.md), Provider settings). The key file it names is mode 600 too.

```ssh-config
Host athena
HostName <login-node>
User <user>
IdentityFile /home/<user>/.ssh/id_ed25519_athena
```

**The front's client keys** are the keys your clients present to it, one per client ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Request path). Generate one without printing it, and build the three files from it:

```
(
  umask 077
  cd ~/.config/hpcburst
  openssl rand -hex 32 > client.key
  { printf '{"keys":[{"id":"me","key":"'; tr -d '\n' < client.key; printf '"}]}\n'; } > client-keys.json
  { printf 'Authorization: Bearer '; cat client.key; } > client.hdr
)
```

`client-keys.json` has the form `{"keys": [{"id": "me", "key": "..."}]}`. `client.hdr` is a header file for `curl -H @file`, so the key stays out of `ps`.

The front's configuration names that file and a directory for usage records ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Parameters). Every other setting has a default:

```
install -d -m 700 "$STATE/usage"
cat > ~/.config/hpcburst/front.json <<EOF
{"clientKeysFile": "$HOME/.config/hpcburst/client-keys.json", "usageDir": "$STATE/usage"}
EOF
```

## 4. Start the host

```
bin/hpcburst up --state-dir "$STATE" --bin-dir "$BIN" --manifests config/crd \
  --front-config ~/.config/hpcburst/front.json \
  --front-mode host-dialed
bin/hpcburst status --state-dir "$STATE"
```

- `--manifests config/crd` installs the framework's CRDs once the host is ready ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), the launcher).
- `up` also installs the CRD deletion guard, on every start. Deleting a framework CRD would delete every resource of its kind and cancel every provider job, and the guard refuses it. `status` prints whether the guard is present. To uninstall the framework, run `bin/hpcburst guard remove --state-dir "$STATE"` first. It is the only step that removes the guard, and `down` and a restart leave it alone.
- `--front-mode host-dialed` takes neither `--front-addr` nor `--front-key`, and `up` refuses either one. `front.json` needs no `dialHomeListen` either: the front's dial-home listener stays on loopback at the port base plus 7, and the front refuses any other address in this mode.
- Without the front flags, `up` warns, the controllers submit no job, and every ModelDeployment shows `FrontNotConfigured`.
- `up` prints the kubeconfig path, `kubeconfig` in the state directory. `status` and `down` take the same `--state-dir`. Without it they use the launcher's default, which is not `"$STATE"` if you chose another directory.
- `status` exits 0 when the host is healthy, 1 when it is not, and 3 when it is stopped. `up` exits 4 when the host is up but an agent build that a profile needs is missing or wrong. The host keeps running, and the message names the command that fixes the build.

In `host-dialed` mode the controller keeps two relays for each running Allocation, one for the agent's control traffic and one for the token tunnel. Each relay is an `ssh` from your host to the login node that runs an `srun` step in the job, and the job's two connections run back over it. [Operations](operations.md#host-dialed-relays) says how to see the relays and what their failures mean.

Use the kubeconfig with the `kubectl` of the k3s you fetched, or with your own. The alias reads `STATE` and `BIN` when you run it:

```
alias hbk='KUBECONFIG="$STATE/kubeconfig" "$BIN/k3s-1.36.5/k3s" kubectl'
```

## 5. Apply a Provider and a ModelDeployment

The files in [`examples/`](../examples) are what this step applies. CI checks each one against the framework's own API definitions, so they stay valid as the API changes. A Provider holds placeholders, so you edit a copy of it and apply the copy. Replace `<account>`, `<grant>` and `<user>`, and point `credentialsRef.file` at the file you wrote in step 3. Keep the Provider's name, because the ModelDeployments name it. The ModelDeployments hold none of your values and are applied as they are.

```
hbk apply -f profiles/athena.yaml
cp examples/provider-athena-a100.yaml ~/.config/hpcburst/provider-athena-a100.yaml
# edit ~/.config/hpcburst/provider-athena-a100.yaml, then:
hbk apply -f ~/.config/hpcburst/provider-athena-a100.yaml
hbk apply -f examples/modeldeployment-athena-qwen25-instruct.yaml
hbk get providers
hbk get modeldeployments
bin/hpcburst status --state-dir "$STATE"
```

The profile tells `status` which agent builds the host needs. Once it is applied, `status` lists `hb-agent-x86_64` and `frpc-x86_64` and marks a missing or mismatched one. That makes the host unhealthy until you fix it with the commands of step 2.

| Example | What it is |
|---|---|
| `provider-athena-a100.yaml` | a batch Provider on Athena `plgrid-gpu-a100` |
| `modeldeployment-athena-qwen25-instruct.yaml` | a small instruct model, served on that Provider |
| `modeldeployment-qwen3-reasoning.yaml` | a reasoning model, on the same Provider |
| `provider-helios-gh200.yaml` | a batch Provider on Helios `plgrid-gpu-gh200` (beta) |
| `modeldeployment-qwen25-instruct.yaml` | the same small instruct model, on the Helios Provider (beta) |
| `provider-llmlab.yaml` | a managed Provider: LLMLab itself, read-only ([optional](#optional-attach-llmlab)) |
| `modeldeployment-llmlab.yaml` | a model LLMLab already serves, attached to a plain name ([optional](#optional-attach-llmlab)) |

The profile ([`profiles/README.md`](../profiles/README.md)) holds what is true for everyone on a platform. A Provider holds what is yours. The rules for a ModelDeployment, such as the pinned checkpoint revision and engine digest, are in [HBEP-3](../hbep/0003-resource-model-and-naming/README.md) and [HBEP-7](../hbep/0007-lifecycle-controller/README.md). Admission refuses a value that breaks one and says which.

The Provider shows `Ready` once its first read of the platform succeeds. The ModelDeployment then submits an Allocation, which is one Slurm job. It waits in Athena's queue, stages the agent and the checkpoint, and starts the engine, which the framework calls an Instance. The wait in the queue depends on the cluster and the partition, and can be days when the cluster is busy. Watch it with `hbk get modeldeployments -w`, or wait for it:

```
hbk wait --for=condition=Ready modeldeployment/athena-qwen25-instruct --timeout=24h
```

## 6. Send a request

The front listens on `127.0.0.1:8000` by default. Ask for the model by its plain name, the ModelDeployment's `spec.model` ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Request path):

```
curl -sS -i http://127.0.0.1:8000/v1/chat/completions \
  -H @$HOME/.config/hpcburst/client.hdr -H 'Content-Type: application/json' \
  -d '{"model": "Qwen/Qwen2.5-1.5B-Instruct", "messages": [{"role": "user", "content": "Say hello in one word."}], "max_tokens": 16}'
```

Before an Instance is ready the front answers 503 with `model_inactive` and `Retry-After: 30` ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Errors). Retry once the ModelDeployment is Ready. The `hpcburst-provider` response header names the Provider that served the request ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Attribution headers). `-i` shows it.

## 7. Stop

Delete what you applied first, while the host still runs, so that the controllers can end the jobs ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Ownership, records and deletion):

```
hbk delete modeldeployment athena-qwen25-instruct
hbk get instances,allocations
```

Deleting a ModelDeployment drains its Instances, which lets requests in flight finish, and cancels an Allocation that is still pending. A started Allocation follows its retention policy: the examples release it after 30 minutes without an Instance ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Retention). When both lists are empty:

```
bin/hpcburst down --state-dir "$STATE"
```

`down` stops the processes it started and confirms that nothing of theirs still listens. It does not call the clusters, so a job left behind runs to its time limit. Check your queue (`squeue`) if the lists were not empty.

## Optional: attach LLMLab

A managed Provider attaches a model that LLMLab already serves, so the front can route to it by the same plain name. It starts nothing. It needs an LLMLab API key.

**The LLMLab Provider's file** is `~/.config/hpcburst/credentials/llmlab.json`, the path that `provider-llmlab.yaml` names. Create it empty with mode 600, as in step 3, then edit it. It is strict JSON with exactly the members `key`, `baseURL` and an optional `expiry` (RFC 3339). `baseURL` ends in `/v1` ([HBEP-4](../hbep/0004-provider-interface/README.md), Provider settings). Put the real key and the gateway's base URL (from LLMLab's documentation) in:

```json
{
  "key": "<llmlab-key>",
  "baseURL": "https://<llmlab-gateway>/v1",
  "expiry": "2027-01-31T00:00:00Z"
}
```

Then apply `profiles/llmlab.yaml`, a copy of `provider-llmlab.yaml` with `<user>` replaced, and `modeldeployment-llmlab.yaml`, as in step 5.

## Helios (beta)

Helios is beta, to run when the GH200 nodes return. The steps are the same, with these changes:

- Write a Helios ssh file, as in step 3, and apply `profiles/helios.yaml`, a copy of `provider-helios-gh200.yaml` and `modeldeployment-qwen25-instruct.yaml`.
- Build the `aarch64` agent as well. Helios `plgrid-gpu-gh200` needs it, and the Helios profile also lists an `x86_64` CPU partition. `hack/build-agents.sh` with no architecture builds both; the release archive's `agent-builds/` already holds both.
- The Helios example asks for QOS `now` (`qos: now`). It limits each job to 12 hours and everything you run at once, your other jobs on that login included, to one GPU, 72 CPUs and 122400Mi of memory. Further jobs can queue behind a running one, but they wait while that GPU is in use, so no quick start is promised ([HBEP-4](../hbep/0004-provider-interface/README.md), QOS limits). Delete the line to use your association's default QOS.

## Beta: Instances behind a router

Opt-in. With `--instance-router envoy`, the controller runs each new Instance's worker behind Envoy ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Inside an Instance), and every job is staged with Envoy's build. The release archive doesn't ship that build, so fetch it into the agent builds first, then start the host with the flag; the other flags are step 4's:

```
hack/fetch-envoy.sh --agent-builds "$STATE/agent-builds"
bin/hpcburst up --state-dir "$STATE" --bin-dir "$BIN" --manifests config/crd \
  --front-config ~/.config/hpcburst/front.json \
  --front-mode host-dialed --instance-router envoy
```

- `up` and `status` then check `envoy-<cpuArch>` for each partition's architecture, against the pinned digest, and name the fetch command when one is missing or wrong.
- Envoy runs bare in the job and needs glibc 2.30 or later on the compute nodes. The agent checks it before it starts Envoy, and refuses an older node, naming the glibc it found and the one Envoy needs.

## Alpha's limits

- **Athena is the alpha platform, and its runs are still being measured.** The runs are tracked in [issue #145](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/145). Expect queue waits and start-up times to move as the profile is tuned.
- **`plgrid-now` is for short tests, not for serving.** The partition gives one GPU and one job at a time, for one hour. Only `profiles/athena-now.yaml` accepts a deployment there: its margins are cut to fit the hour, and the image and the checkpoint should already be staged, because a cold start can run past them. With `profiles/athena.yaml` the deployment is Degraded with `PartitionTooShort` and gets no job. Real serving uses `plgrid-gpu-a100`.
- **In `host-dialed` mode no job dials your host,** so a host that compute nodes cannot reach works with the Provider's ssh credentials alone ([HBEP-6](../hbep/0006-instance-reachability/README.md), Host-dialed mode). The price is that the controller owns the relays. They die with its process, so a restart cuts the token streams on them, and the new controller starts the relays again within seconds (1 s, doubling to 30 s). A Provider has at most 2 Allocations, pending or running, so a third waits at `SubmitLimitReached`. Only one host-dialed Provider runs per login host: a second Provider on the same login node is refused, and shows `Ready` false. Each relay is an ssh session and an `srun` client on the login node, and neither computes anything ([HBEP-4](../hbep/0004-provider-interface/README.md), open question 14).
- **`direct` and `ssh` modes need a route that you provide.** In `direct` mode the front's dial-home listener (`dialHomeListen` in the front's configuration) must be reachable from the compute nodes, and `up` takes `--front-addr`. In `ssh` mode each job holds two `ssh -N -L` connections to a gateway with a restricted key, which you create and authorize once per cluster. `up` takes `--front-addr` and `--front-key`, and only checks that the key file exists. The key's lines name the launcher's port block, so another `--port-base` needs new keys ([HBEP-6](../hbep/0006-instance-reachability/README.md), Dial-home modes). Nothing in this repository sets either route up for you.
- **Beta, and off unless switched on:** the Envoy router in front of an Instance's engine ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md)), the bulk path for large replies ([HBEP-6](../hbep/0006-instance-reachability/README.md), The bulk path), and the relay keeper that keeps the relays across a controller restart. The `up` command in step 4 switches on none of them.
- **One user, one host.** The host is a single-user k3s on one machine ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md)). A team option is not part of alpha.
- **One partition per Provider**, and a Provider names exactly one ([HBEP-4](../hbep/0004-provider-interface/README.md), open question 12). A second partition is a second Provider.
- **Helios `plgrid` hosts no model.** It is a CPU partition, in the Helios profile for conformance runs and short checks only ([`profiles/README.md`](../profiles/README.md)).
- **Sano is not shipped:** no PlatformProfile for it is in `profiles/`, only its draft.
- **A managed Provider only lists.** It reads LLMLab's catalog once a minute and routes to it. It starts and stops nothing ([HBEP-4](../hbep/0004-provider-interface/README.md), Managed provider in alpha).
- **Billing is recorded, not read.** The `grant` field is kept, but nothing in alpha charges against it.
