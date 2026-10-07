# `ClusterSchema.WithTransaction`: a typed handler failure loses its reply

`effect@4.0.1` (also `4.0.0`; the code is unchanged on `main`). Node 22.5+
(uses `node:sqlite`, so nothing to install or run besides this).

```bash
pnpm install
pnpm repro
```

## What happens

A persisted entity RPC annotated with `ClusterSchema.WithTransaction`, whose
handler fails with its declared error, never gets its reply stored. The repro
registers the same RPC twice, with and without the annotation, and calls each
once and then again with the same primary key:

```
WithoutTx
  first call : Failure(Rejected)
  retry      : Failure(Rejected)
  stored     : processed=true replies=1
WithTx
  first call : Failure(Rejected)
  retry      : TimeoutError after 3s
  stored     : processed=false replies=0
```

With the annotation:

- the first caller gets the failure (handed over in memory);
- the request stays unprocessed with no reply row, so a retry with the same
  primary key, or a caller on another runner, waits on storage forever;
- after a restart the request is delivered again, the handler runs again,
  fails again and is rolled back again, on every restart.

Expected: the failure reply is stored like any other reply. The handler's own
writes rolling back on failure is reasonable; the reply and the message's
processed flag rolling back with them is not.

## Why

`entityManager.ts` (`requestWriteOptions`, line 598) passes
`storage.withTransaction` as the RPC server's `onRequest`.
`RpcServer.ts` builds the request effect as
`Effect.onExit(handler, exit => write reply)` (line 291) and then wraps the
whole thing in `onRequest` (line 327). The reply write is therefore inside the
transaction, and `withTransaction` rolls back when the handler's exit is a
failure, taking the reply with it.

The existing test (`Sharding.test.ts`, "WithTransaction is propagated to the
entity handler") only covers success. The workflow engine's own transactional
RPC (`activity`) never fails: it turns failures into a successful
`Workflow.Result`, so it doesn't hit this.

## Possible fixes

- Wrap only the handler in the transaction and store a failure reply in its
  own transaction after the rollback, or
- commit the transaction on a typed failure (`Fail`) and roll back only on
  defects and interrupts, or at least
- document that a handler under `WithTransaction` must not fail.

Workaround today: return rejections in the success channel, for example
`success: Schema.Result(A, E)`, and decide before writing.
