# Authentication — analysis and design

This document analyses how Outboxer authenticates to Google Pub/Sub and AWS SQS
across the deployment scenarios we want to support, what works today, and what
needs to change. It is a design note, not a how-to (operator instructions will
follow once the design is settled).

## Goals

Outboxer should authenticate using each provider's **standard, keyless**
credential mechanism wherever possible, and avoid long-lived secrets. Concretely
we want to support:

- Deploy on **GCP Cloud Run** → publish to **Pub/Sub** and to **SQS**.
- Deploy on **AWS container services** (ECS/Fargate, ECS/EC2, EKS, App Runner,
  Elastic Beanstalk, plain EC2) → publish to **SQS** and to **Pub/Sub**.
- Run **locally** after `gcloud auth application-default login` and/or
  `aws sso login` → publish to both.

## How the two SDKs find credentials

Outboxer does not implement auth itself; it relies on each cloud SDK's default
credential resolution. Understanding both chains is the key to the whole design.

### Google — Application Default Credentials (ADC)

`pubsub.NewClient(ctx, projectID, …)` uses ADC, which resolves in order:

1. `GOOGLE_APPLICATION_CREDENTIALS` — path to a credentials JSON. This can be a
   service-account key **or** an *external account* (Workload Identity
   Federation) config. This is the hook that makes AWS→GCP work without keys.
2. gcloud user credentials from `gcloud auth application-default login`
   (`~/.config/gcloud/application_default_credentials.json`).
3. The **metadata server** on GCP compute (Cloud Run, GKE, GCE) — the attached
   service account.

**Project resolution** is separate from credentials. Today Outboxer calls
`NewClient(ctx, "")`, so the project is detected from ADC / `GOOGLE_CLOUD_PROJECT`.
A short topic name (`user-events`) is resolved against that project; a full
resource name (`projects/PROJECT/topics/user-events`) is self-contained.

### AWS — default credential chain

`config.LoadDefaultConfig(ctx)` resolves in order:

1. Environment variables (`AWS_ACCESS_KEY_ID`, …).
2. Shared config/credentials files, including **SSO** profiles
   (`aws sso login` + `AWS_PROFILE`).
3. **Web identity** token file (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`) —
   used by EKS IRSA, and the hook that makes GCP→AWS work without keys.
4. **Container credentials** endpoint (`AWS_CONTAINER_CREDENTIALS_*`) — ECS task
   roles, App Runner, EKS Pod Identity.
5. **IMDS** instance metadata — EC2 instance profiles, ECS-on-EC2, Beanstalk.

Outboxer additionally supports `AWS_ROLE_ARN` to **assume a role** on top of
whatever the chain resolved (role chaining, e.g. cross-account SQS access).

The important consequence: **every AWS compute platform** (Fargate, ECS/EC2,
EKS, App Runner, Beanstalk, EC2) surfaces its identity through this one chain, so
"run on any AWS service" needs no per-service code — `LoadDefaultConfig` already
covers them all.

## Scenario matrix

| Run environment | Target | Mechanism | Works today? |
| --- | --- | --- | --- |
| GCP Cloud Run | Pub/Sub | ADC via metadata server | ✅ |
| GCP Cloud Run | SQS | GCP→AWS federation (web identity) | ⚠️ needs work |
| AWS (any) | SQS | AWS default chain (task/instance role) | ✅ |
| AWS (any) | Pub/Sub | AWS→GCP federation (external account) | ✅ via config, no code |
| Local | Pub/Sub | gcloud ADC login | ✅ |
| Local | SQS | aws sso login (+ `AWS_PROFILE`) | ✅ |
| Local | both | the two chains are independent | ✅ |

The native cases (run on cloud X, publish to cloud X) and all local cases work
with today's code. The two cross-cloud cases are the design work.

## Native cases (already working)

- **AWS → SQS:** the task/instance role is picked up by `LoadDefaultConfig`. The
  role needs `sqs:SendMessage` (and `sqs:GetQueueUrl` is not required since we
  pass full queue URLs). Set `AWS_REGION` if not inferable.
- **GCP → Pub/Sub:** the Cloud Run service account is picked up via the metadata
  server. It needs `roles/pubsub.publisher` on the topic(s).
- **Local:** `gcloud auth application-default login` covers Pub/Sub;
  `aws sso login` (with the right `AWS_PROFILE`) covers SQS. They use different
  credential stores, so both can be active at once.

## Cross-cloud case 1: AWS → GCP Pub/Sub

**This already works with no code changes**, because Google's ADC understands
*external account* credentials (Workload Identity Federation).

Setup (operator side):

1. Create a GCP **workload identity pool** with an **AWS provider** that trusts
   the AWS account/role the container runs as.
2. Grant the federated identity `roles/pubsub.publisher` on the topics (via a
   GCP service account it impersonates, or direct IAM).
3. Generate the credential config with
   `gcloud iam workload-identity-pools create-cred-config …`, ship the JSON to
   the container, and set `GOOGLE_APPLICATION_CREDENTIALS` to it.

At runtime the Google library reads the config, pulls the **AWS** credentials
from the standard AWS chain, signs an STS `GetCallerIdentity` request, exchanges
it with GCP STS for a federated access token, and uses that to publish. Outboxer
is unaware of any of this.

**Likely gap:** project resolution. The detected project may not be the target
Pub/Sub project. We should let operators set the project explicitly (see
Proposed changes) or require full `projects/…/topics/…` destinations.

## Cross-cloud case 2: GCP Cloud Run → AWS SQS

This is the one case that needs code. The AWS default chain has no notion of a
GCP identity, so we must feed AWS a federated credential.

The clean, keyless approach is **AssumeRoleWithWebIdentity** using a Google-issued
OIDC token:

1. In AWS IAM, register an **OIDC identity provider** for Google
   (issuer `https://accounts.google.com`).
2. Create an IAM role that trusts that provider, scoped to the Cloud Run service
   account's `sub`, with `sqs:SendMessage` on the target queues.
3. At runtime, fetch a Google **ID token** from the Cloud Run metadata server
   (`…/instance/service-accounts/default/identity?audience=<role/aud>`) and call
   STS `AssumeRoleWithWebIdentity` with it.

The AWS SDK supports this via `stscreds.NewWebIdentityRoleProvider`, which takes a
`TokenRetriever` (`GetIdentityToken() ([]byte, error)`). We would implement a
retriever that fetches the Google ID token, and wire it up when configured. The
SDK's built-in `AWS_WEB_IDENTITY_TOKEN_FILE` path is not sufficient on its own
because it reads a *static file*, and nothing on Cloud Run keeps a Google ID
token (≈1h lifetime) refreshed in that file.

Interim options without new code:
- **Static AWS access keys** as env vars (works everywhere, but long-lived
  secrets — discouraged).
- A sidecar that periodically writes a fresh Google ID token to
  `AWS_WEB_IDENTITY_TOKEN_FILE` (operationally awkward).

## The existing `AWS_ROLE_ARN` feature

`AWS_ROLE_ARN` assumes a role *on top of* the resolved base credentials. It is
orthogonal to the federation above and useful for cross-account SQS (the
task/federated identity assumes a role in the queue's account). It should remain,
and ideally compose with web-identity (base = web identity → assume target role).

## Proposed changes to Outboxer

Minimal, mostly additive:

1. **Explicit Pub/Sub project** — add `PUBSUB_PROJECT_ID` (passed to
   `NewClient`) so cross-cloud and ambiguous-ADC setups can target the right
   project without relying on detection. Full `projects/…/topics/…` destinations
   remain supported as an alternative.
2. **GCP→AWS web identity** — add an opt-in AWS web-identity provider that
   sources a Google OIDC ID token (audience configurable) and feeds
   `AssumeRoleWithWebIdentity`. Compose with the existing `AWS_ROLE_ARN`. Likely
   config: `AWS_WEB_IDENTITY=google` (or similar) + an audience/role setting.
3. **Lean on standard env vars** — document that `GOOGLE_APPLICATION_CREDENTIALS`
   (incl. external-account configs), `AWS_PROFILE`, `AWS_WEB_IDENTITY_TOKEN_FILE`,
   `GOOGLE_CLOUD_PROJECT`, etc. are honored, rather than inventing parallel
   config. The current custom options (`AWS_REGION`, `AWS_ROLE_*`,
   `PUBSUB_API_ENDPOINT`) stay.
4. **Document required IAM** — `roles/pubsub.publisher` for Pub/Sub;
   `sqs:SendMessage` for SQS; plus the federation trust setup for each
   cross-cloud direction.

## Decisions

- **Static AWS keys: supported, but not recommended.** The keyless web-identity
  path is the recommended way to reach AWS from GCP. Static keys still work
  through the AWS SDK's standard env vars (`AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY`), so they are documented as an escape hatch for quick
  starts and restricted environments, clearly marked as long-lived secrets. No
  Outboxer code is needed for them.
- **GCP→AWS token source: metadata server only.** Outboxer fetches the Google
  OIDC token from the GCP metadata server, which covers Cloud Run (the primary
  target), GCE, and GKE with Workload Identity. File-based tokens (e.g. GKE
  federated to the cluster's own OIDC issuer, or custom brokers) are handled by
  the AWS SDK's native `AWS_WEB_IDENTITY_TOKEN_FILE` path with no Outboxer code,
  and Kubernetes keeps that file fresh.
- **`PUBSUB_PROJECT_ID`: optional, with ADC detection as the fallback.** Setting
  it (or using full `projects/…/topics/…` destinations) is how you target a
  specific project when detection is ambiguous, e.g. cross-cloud.

## Configuration

GCP→AWS keyless federation is enabled with:

| Variable | Purpose |
| --- | --- |
| `AWS_WEB_IDENTITY_PROVIDER` | Set to `google` to assume an AWS role using a Google OIDC token. |
| `AWS_WEB_IDENTITY_AUDIENCE` | Audience requested in the Google ID token; must match the AWS IAM OIDC provider. |
| `AWS_ROLE_ARN` | The AWS role to assume (the role whose trust policy trusts the Google provider). |

When `AWS_WEB_IDENTITY_PROVIDER` is empty (the default), `AWS_ROLE_ARN` keeps its
existing meaning: a role assumed on top of the AWS default credential chain
(role chaining). `AWS_ROLE_SESSION_NAME` applies to both paths; sessions last
one hour and refresh five minutes before expiry.

`PUBSUB_PROJECT_ID` sets the Google Cloud project for Pub/Sub; leave it empty to
detect it from ADC.

## Required IAM

- **Pub/Sub:** the identity needs `roles/pubsub.publisher` on the target topics.
- **SQS:** the identity needs `sqs:SendMessage` on the target queues. Full queue
  URLs are used, so `sqs:GetQueueUrl` is not required.
- **AWS→GCP federation:** a workload identity pool with an AWS provider, plus the
  Pub/Sub permission above on the federated identity.
- **GCP→AWS federation:** an AWS IAM OIDC provider for Google
  (`accounts.google.com`) and a role whose trust policy allows the Google service
  account's subject, with the SQS permission above.
