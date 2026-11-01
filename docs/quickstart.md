# Quickstart

This takes you from a clone to one request through the front. You start a control-plane host on your own machine, describe where a model may run (a Provider) and which model you want (a ModelDeployment), and send an OpenAI-style request to the front by the model's plain name. The framework submits and supervises the HPC jobs.

Read [HBEP-2](../hbep/0002-scope-and-user-story/README.md) first for what the framework is for. This page is the shortest path; each rule it relies on lives in an HBEP, and the steps link there instead of repeating it. The framework is alpha: [Alpha's limits](#alphas-limits) at the end lists what to expect.

Commands run from the repository root. `<user>`, `<account>`, `<grant>` and `<login-node>` stand for your own values; nothing private belongs in a manifest you share.

## 1. What you need

- **A host** for the control plane, as [HBEP-11](../hbep/0011-kubernetes-control-plane/README.md) (Single-user host) describes it: a Linux machine that you use as a plain user, with no root and no system change. It runs a user-level k3s (about 400 MiB of memory), the controllers, the front and frps. They listen on loopback only, on a block of 13 ports starting at 29220 (`hpcburst up -h`, `--port-base`), which must be free.
- **Go 1.26 or later**, to build the binaries ([development.md](development.md) lists the pins), plus `curl`, `openssl` and an OpenSSH client. The frps download in step 2 is pinned for Linux on x86_64; on another architecture, supply your own frps with `hpcburst up --frps-bin`, which checks it against the launcher's pin.
- **For a batch Provider** (Helios, Athena): a grant there, an ssh key for the login node that works without a passphrase prompt (the transport runs ssh in batch mode, [HBEP-4](../hbep/0004-provider-interface/README.md), Transport), and a login node that has been reached by hand once, so its host key is in your `known_hosts` (the transport always checks it).
- **For the LLMLab Provider**: an LLMLab API key.
- **A way for the jobs to reach your host**, the dial-home route. See [Alpha's limits](#alphas-limits); you choose the mode in step 4.

## 2. Build and fetch the binaries

```
mkdir -p bin
go build -o bin/ ./cmd/hpcburst ./cmd/hpcburst-controller ./cmd/hpcburst-front
hack/fetch-k3s.sh
hack/fetch-frp.sh ~/.cache/hpcburst/k3s/frp-0.71.1
```

- `hpcburst up` looks for `hpcburst-controller` and `hpcburst-front` beside itself, which is why they share `bin/`.
- `hack/fetch-k3s.sh` puts the pinned k3s releases under `~/.cache/hpcburst/k3s/k3s-<version>/`, the default `--bin-dir`, and checks their sha256 against the launcher's pins ([HBEP-15](../hbep/0015-dependencies-and-provenance/README.md)). `hack/fetch-frp.sh DIR` places the pinned `frps` (and `frpc`) under `frp-0.71.1/` in the same directory, where `up` finds it.

The jobs also need an allocation agent and `frpc`, built for each CPU architecture of the partitions you will use. The Provider's controller stages them from `<state>/agent-builds/`, the state directory being `~/.local/state/hpcburst` by default ([HBEP-4](../hbep/0004-provider-interface/README.md), Submission):

```
STATE=~/.local/state/hpcburst
install -d -m 700 "$STATE" "$STATE/agent-builds"
hack/fetch-frp.sh --agent-builds "$STATE/agent-builds"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o "$STATE/agent-builds/hb-agent-aarch64" ./cmd/hb-agent
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$STATE/agent-builds/hb-agent-x86_64" ./cmd/hb-agent
```

Helios `plgrid-gpu-gh200` needs `aarch64`; Athena `plgrid-gpu-a100` needs `x86_64`. The build's sha256 is the digest the controller puts in each Allocation, so build with the same toolchain each time.

## 3. Write your credentials files

The API never holds a credential, only the path of a file. Each file is mode 600 and owned by the user who runs the controllers; a file that others can read is refused, and the Provider shows `CredentialsInvalid` ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Credentials). Create each file empty with the right mode, then edit it, so a secret never appears on a command line:

```
install -d -m 700 ~/.config/hpcburst ~/.config/hpcburst/credentials
install -m 600 /dev/null ~/.config/hpcburst/credentials/helios-gh200.ssh
```

**A batch Provider's file** is an ssh client configuration with at most one `Host` line. It takes `HostName`, `User` and `IdentityFile`, and optionally `Port`, `UserKnownHostsFile`, `StrictHostKeyChecking yes` and `IdentitiesOnly yes`; any other keyword, `ProxyJump` and the forwards included, is refused ([HBEP-4](../hbep/0004-provider-interface/README.md), Provider settings). The key file it names is mode 600 too.

```ssh-config
Host helios
HostName <login-node>
User <user>
IdentityFile /home/<user>/.ssh/id_ed25519_helios
```

**The LLMLab Provider's file** is strict JSON with exactly the members `key`, `baseURL` and an optional `expiry` (RFC 3339), and `baseURL` ends in `/v1` ([HBEP-4](../hbep/0004-provider-interface/README.md), Provider settings). Put the real key and the gateway's base URL (from LLMLab's documentation) in by editing the file:

```json
{
  "key": "<llmlab-key>",
  "baseURL": "https://<llmlab-gateway>/v1",
  "expiry": "2027-01-31T00:00:00Z"
}
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

`client-keys.json` has the form `{"keys": [{"id": "me", "key": "..."}]}`; `client.hdr` is a header file for `curl -H @file`, so the key stays out of `ps`.

The front's configuration names that file and a directory for usage records ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Parameters). Every other setting has a default:

```
install -d -m 700 "$STATE/usage"
cat > ~/.config/hpcburst/front.json <<EOF
{"clientKeysFile": "$HOME/.config/hpcburst/client-keys.json", "usageDir": "$STATE/usage"}
EOF
```

## 4. Start the host

```
bin/hpcburst up --manifests config/crd \
  --front-config ~/.config/hpcburst/front.json \
  --front-mode ssh --front-addr <front-host> --front-key ~/.config/hpcburst/dial-home-key
bin/hpcburst status
```

- `--manifests config/crd` installs the framework's CRDs once the host is ready ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), the launcher).
- `--front-mode`, `--front-addr` and, in `ssh` mode, `--front-key` (the path of the restricted dial-home key, recorded and never read) say how jobs reach the front. With none of them, `up` warns, the controllers submit no job, and every ModelDeployment shows `FrontNotConfigured`. `--front-mode direct --front-addr <host>:<port>` is the other mode, and also needs `dialHomeListen` in `front.json`. [Alpha's limits](#alphas-limits) says what each mode asks of you.
- `up` prints the kubeconfig path, `~/.local/state/hpcburst/kubeconfig` by default. `status` exits 0 when the host is healthy, 1 when it is not, and 3 when it is stopped.

Use that kubeconfig with the `kubectl` of the k3s you fetched, or with your own:

```
alias hbk='KUBECONFIG=~/.local/state/hpcburst/kubeconfig ~/.cache/hpcburst/k3s/k3s-1.36.5/k3s kubectl'
```

## 5. Apply a Provider and a ModelDeployment

The examples in [`examples/`](../examples) are what this step applies. CI admits each one against the real CRDs, so they stay valid as the contracts change. Copy a Provider and replace its placeholders (`<account>`, `<grant>`, `<user>`); its `credentialsRef.file` is a file from step 3.

```
hbk apply -f profiles/helios.yaml -f profiles/athena.yaml -f profiles/llmlab.yaml
hbk apply -f examples/provider-helios-gh200.yaml
hbk apply -f examples/modeldeployment-qwen25-instruct.yaml
hbk get providers
hbk get modeldeployments
```

| Example | What it is |
|---|---|
| `provider-helios-gh200.yaml` | a batch Provider on Helios `plgrid-gpu-gh200` |
| `provider-athena-a100.yaml` | a batch Provider on Athena `plgrid-gpu-a100` |
| `provider-llmlab.yaml` | a managed Provider: LLMLab itself, read-only |
| `modeldeployment-qwen25-instruct.yaml` | a small instruct model, served on a batch Provider |
| `modeldeployment-qwen3-reasoning.yaml` | the same with a reasoning parser, on Athena |
| `modeldeployment-llmlab.yaml` | a model LLMLab already serves, attached to a plain name |

The profiles ([`profiles/README.md`](../profiles/README.md)) hold what is true for everyone on a platform; a Provider holds what is yours. The rules for a ModelDeployment, such as the pinned checkpoint revision and engine digest, are in [HBEP-3](../hbep/0003-resource-model-and-naming/README.md) and [HBEP-7](../hbep/0007-lifecycle-controller/README.md); admission refuses a value that breaks one and says which.

The Provider shows `Ready` once its first read of the platform succeeds. A served ModelDeployment then submits an Allocation, which waits in the cluster's queue (how long depends on the cluster, the partition and the QOS: [HBEP-4](../hbep/0004-provider-interface/README.md), QOS limits), stages the agent and the checkpoint, and starts the engine. Watch it with `hbk get modeldeployments -w`, or wait for it:

```
hbk wait --for=condition=Ready modeldeployment/helios-qwen25-instruct --timeout=3h
```

## 6. Send a request

The front listens on `127.0.0.1:8000` by default. Ask for the model by its plain name, the ModelDeployment's `spec.model` ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Request path):

```
curl -sS -i http://127.0.0.1:8000/v1/chat/completions \
  -H @$HOME/.config/hpcburst/client.hdr -H 'Content-Type: application/json' \
  -d '{"model": "Qwen/Qwen2.5-1.5B-Instruct", "messages": [{"role": "user", "content": "Say hello in one word."}], "max_tokens": 16}'
```

Before an Instance is ready the front answers 503 with `model_inactive` and `Retry-After: 30` ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Errors); retry once the ModelDeployment is Ready. The `hpcburst-provider` response header names the Provider that served the request ([HBEP-5](../hbep/0005-llmlab-compatible-front/README.md), Attribution headers); `-i` shows it.

## 7. Stop

Delete what you applied first, while the host still runs, so the controllers can end the jobs ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md), Ownership, records and deletion):

```
hbk delete modeldeployment helios-qwen25-instruct
hbk get instances,allocations
```

Deleting a ModelDeployment drains its Instances, which lets requests in flight finish, and cancels an Allocation that is still pending. A started Allocation follows its retention policy: the examples release it after 30 minutes without an Instance ([HBEP-7](../hbep/0007-lifecycle-controller/README.md), Retention). When both lists are empty:

```
bin/hpcburst down
```

`down` stops the processes it started and confirms that nothing of theirs still listens. It does not call the clusters, so a job left behind runs to its time limit; check your queue (`squeue`) if the lists were not empty.

## Alpha's limits

- **The dial-home route is yours to provide.** Jobs on compute nodes dial home to your host ([HBEP-6](../hbep/0006-instance-reachability/README.md), Dial-home modes). In `direct` mode the front's dial-home listener (`dialHomeListen` in the front's configuration) must be reachable from the compute nodes. In `ssh` mode each job holds two `ssh -N -L` connections to a gateway with a restricted key, which you create and authorize once per cluster; the key's lines name the launcher's port block, so another `--port-base` needs new keys ([HBEP-6](../hbep/0006-instance-reachability/README.md), Ssh mode). The key's staging on the cluster is in [HBEP-4](../hbep/0004-provider-interface/README.md), Submission. Nothing in this repository sets either route up for you.
- **One user, one host.** The host is a single-user k3s on one machine ([HBEP-11](../hbep/0011-kubernetes-control-plane/README.md)); a team option is not part of alpha.
- **One partition per Provider**, and a Provider names exactly one ([HBEP-4](../hbep/0004-provider-interface/README.md), open question 12). A second partition is a second Provider.
- **Short-job partitions host no model.** Helios `plgrid` and Athena `plgrid-now` are in the profiles for conformance runs and short checks only ([`profiles/README.md`](../profiles/README.md)).
- **Sano is not shipped**, and no profile for it exists yet.
- **A managed Provider only lists.** It reads LLMLab's catalog once a minute and routes to it; it starts and stops nothing ([HBEP-4](../hbep/0004-provider-interface/README.md), Managed provider in alpha).
- **Billing is recorded, not read.** The `grant` field is kept, but nothing in alpha charges against it.
- **Real runs are still being measured.** The first runs on Helios and Athena are tracked in the repository's issues; expect queue waits and start-up times to move as the profiles are tuned.
