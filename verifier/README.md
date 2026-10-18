# Acked-loss verifier

Runs beside the OpenMessaging Benchmark during the broker-kill cells and answers one question the
benchmark cannot: did any message the broker acknowledged fail to come back? Extracted from the
KubeMQ server repository's connector-resiliency harness (ten verifiers over one shared ledger);
only the Kafka adapter is carried here. It uses a plain Kafka client (franz-go), so it runs
unchanged against Apache Kafka and against KubeMQ.

```
ledger/               the instrument: JSONL format, verdict rules (PASS / FAIL / VOID), unit tests
cmd/kafka-verifier/   the Kafka adapter
```

## Building

```bash
cd verifier
go build -o bin/kafka-verifier ./cmd/kafka-verifier
go test ./ledger
```

## The one rule that matters

**No green counts until the verifier has been shown going red.** A verifier that has never failed has
not been shown to work — it has only been shown to agree. Every adapter takes `-drop-nth N`, which
silently swallows every Nth message **on the read side**: the same shape as a message the cluster
failed to return.

The gate lies to the **ledger only, never to the broker**. Every adapter still acks, accepts or
deletes each delivery it receives even when the gate tells the ledger to forget it. Skipping the
acknowledgement would change the server's state as well, making the gate a different experiment from
the one it claims to be — and the redelivery would then surface as a duplicate the run invented.

## Red-capability evidence — 2026-08-01

All ten run against a live single-node `next`-engine broker, 200 messages each. Every connector was
enabled simultaneously except AMQP 1.0, which shares port 5672 with AMQP 0-9-1 and took its own pass.

| # | verifier | what counts as acked | GREEN | RED (`-drop-nth`) |
|---|---|---|---|---|
| 1 | `grpc-verifier` | `SendEventStore` → `Sent=true` | 200/200 PASS | 50 → FAIL, 4 missing, ids exact |
| 2 | `rest-verifier` | HTTP 200 + `is_error=false` | 200/200 PASS | 60 → FAIL, 3 missing |
| 3 | `mqtt-verifier` | QoS 1 `PUBACK` | 200/200 PASS | 45 → FAIL, 4 missing |
| 4 | `amqp-verifier` | `basic.ack` in confirm mode | 200/200 PASS | 35 → FAIL, 5 missing |
| 5 | `amqp10-verifier` | disposition `accepted` | 200/200 PASS | 15 → FAIL, 13 missing |
| 6 | `stomp-verifier` | `RECEIPT` frame | 200/200 PASS | 55 → FAIL, 3 missing |
| 7 | `kafka-verifier` | `acks=all` + assigned offset | 200/200 PASS | 40 → FAIL, 5 missing |
| 8 | `aws-verifier` | `SendMessage` → `MessageId` | 200/200 PASS | 25 → FAIL, 8 missing |
| 9 | `gcp-verifier` | `Publish` → server id | 200/200 PASS | 30 → FAIL, 6 missing |
| 10 | `ce-verifier` | HTTP 2xx on `/ce/send/event-store` | 200/200 PASS | 20 → FAIL, 10 missing |

Every red run named the exact ids it had swallowed, not merely a count. `go vet` clean on all eleven
packages; the ledger's 10 unit tests cover the verdict rules including both VOID paths, which no
amount of dropped messages can reach.

## Verdict rules

Computed, never hand-written. See `ledger/verdict.go`.

| outcome | exit | rule |
|---|---|---|
| PASS | 0 | every acked id was observed |
| FAIL | 1 | any acked id absent. No tolerance, no threshold |
| VOID | **2** | the instrument was unusable, so the run says nothing either way |

VOID gets its own exit code deliberately. A caller that treated any non-zero as failure would turn
"we learned nothing" into "the product broke".

Three ways to reach VOID: a caller-supplied reason (digest drift mid-scenario); zero acked on a
scenario that claimed load (an instrument pointed at nothing agrees with everything); and an empty
read with non-zero acks (reader-shaped, not loss-shaped).

Duplicates — the same id twice, or an id observed that was never acked — are **recorded and never
failed**. That is a retried publish whose first ack was lost, which is correct at-least-once
behaviour.

## Excluded from every loss verdict, by protocol

Not overlooked — excluded, because a missing message on these paths is correct behaviour and counting
it would make the instrument report defects that are not defects: MQTT QoS 0, STOMP `SEND` without a
receipt header, AMQP publishes outside confirm mode, and native `SendEvents` (non-store pub/sub).

## The one-shot hazard

Connectors 1, 2 and 10 read through the Events Store, where a groupless subscribe attaches to a
**shared replicated durable cursor** and `StartFromFirstEvent` is honoured only on the first-ever
attach — an explicit group does not buy a virgin cursor either. Those three therefore generate a
fresh channel per run from a timestamp, so reuse is impossible by construction rather than by
discipline.

MQTT, STOMP, AMQP 0-9-1 and AMQP 1.0 have no replay at all: their readers attach **before** load and
there is no second chance to read. That makes them the most exposed readers during chaos, which is
the point — but it also means a VOID from those four needs triage, because "the reader died" and
"messages were lost" look similar from the ledger alone.

## Targeting a remote broker

Every verifier takes `-addr`. For the campaign the ports come from the NodePort Services, which are
**read back from the cluster**, never assumed — the CRD has no `nodePort` field for connectors, so
the assigned ports are arbitrary.

Related: the traffic-runner skills now honour `CT_TARGET_HOST` for the same purpose, and
`setup_broker.sh` refuses to launch a local broker when it is set to a remote host.
