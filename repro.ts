/**
 * A persisted entity RPC annotated with `ClusterSchema.WithTransaction` whose
 * handler fails with its declared typed error never gets a stored reply.
 *
 * The same entity and RPC are registered twice, with and without
 * `WithTransaction`, and called the same way: once, then again with the same
 * primary key (a retry, deduplicated by the mailbox).
 */
import { NodeRuntime } from "@effect/platform-node"
import * as NodeCrypto from "@effect/platform-node/NodeCrypto"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer, Schema } from "effect"
import { ClusterSchema, Entity, SingleRunner } from "effect/cluster"
import { Rpc } from "effect/rpc"
import { SqlClient } from "effect/sql"

class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {}) {}

const Reject = Rpc.make("Reject", {
  payload: { requestId: Schema.String },
  primaryKey: ({ requestId }) => requestId,
  error: Rejected
}).annotate(ClusterSchema.Persisted, true)

const WithTx = Entity.make("WithTx", [Reject]).annotateRpcs(ClusterSchema.WithTransaction, true)
const WithoutTx = Entity.make("WithoutTx", [Reject])

const handlers = { Reject: () => Effect.fail(new Rejected()) }

const Entities = Layer.mergeAll(WithTx.toLayer(handlers), WithoutTx.toLayer(handlers))

const ClusterLive = SingleRunner.layer({ runnerStorage: "memory" }).pipe(
  Layer.provide(NodeCrypto.layer),
  Layer.provideMerge(SqliteClient.layer({ filename: ":memory:" }))
)

const describe = (exit: unknown) => JSON.stringify(exit)

const probe = Effect.fnUntraced(function*(entity: typeof WithTx | typeof WithoutTx) {
  const sql = yield* SqlClient.SqlClient
  const client = (yield* entity.client)("entity-1")
  const call = client.Reject({ requestId: "request-1" }).pipe(Effect.timeout("3 seconds"), Effect.exit)

  const first = yield* call
  const retry = yield* call
  const [stored] = yield* sql<{ processed: number; replies: number }>`
    SELECT m.processed AS processed, COUNT(r.id) AS replies
    FROM cluster_messages m LEFT JOIN cluster_replies r ON r.request_id = m.request_id
    WHERE m.entity_type = ${entity.type} GROUP BY m.processed`

  console.log(`\n${entity.type}`)
  console.log(`  first call : ${describe(first)}`)
  console.log(`  retry      : ${describe(retry)}`)
  console.log(`  stored     : processed=${Boolean(stored?.processed)} replies=${stored?.replies ?? 0}`)
  return { first, retry, stored }
})

const main = Effect.gen(function*() {
  const without = yield* probe(WithoutTx)
  const withTx = yield* probe(WithTx)

  const replyLost = !withTx.stored?.processed && !!without.stored?.processed
  console.log(
    replyLost
      ? "\n✗ With WithTransaction the failure reply is rolled back with the handler's writes: the request stays unprocessed and callers time out.\n"
      : "\n✓ Both variants behave the same.\n"
  )
  if (replyLost) process.exitCode = 1
})

main.pipe(Effect.provide(Entities.pipe(Layer.provideMerge(ClusterLive))), NodeRuntime.runMain)
