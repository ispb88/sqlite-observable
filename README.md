# Observable SQLite

SQLite in the browser, stored in the [Origin Private File System](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system), with **live queries**: subscribe to a `SELECT` and get new results whenever a table it reads from changes, including changes made from other tabs.

Built on [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) and RxJS.

```ts
gateway.observe("app", "SELECT * FROM todos WHERE done = 0").subscribe(message => {
    // fires on load, then again after every write to `todos`
})
```

## Installation

```sh
npm install @ispb/observable-sqlite rxjs
```

`rxjs` (v7.8+) is a peer dependency. The package is ESM only.

wa-sqlite is installed from a pinned GitHub commit, not the npm registry. If your npm settings block git dependencies, allow them for this install (this repo uses `allow-git=root` in `.npmrc`).

### Bundler setup

wa-sqlite loads a WebAssembly file at runtime, which some bundlers' pre-bundling breaks. With Vite, exclude it from dependency optimisation:

```ts
// vite.config.ts
export default defineConfig({
    optimizeDeps: {
        exclude: ["wa-sqlite"],
    },
})
```

### Runtime requirements

Run the gateway **inside a dedicated Web Worker**. The OPFS storage backend uses synchronous file access handles, which browsers only provide in dedicated workers. To use the database from the main thread, see [Using it from the main thread](#using-it-from-the-main-thread).

## Quick start

```ts
// db.worker.ts
import { WaSqliteGateway, isFulfilled, zipSqliteResults } from "@ispb/observable-sqlite"
import { filter, map } from "rxjs"

const gateway = new WaSqliteGateway({})

await gateway.query("app", "setup", `
    CREATE TABLE IF NOT EXISTS todos (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0
    )
`)

interface Todo {
    readonly id: number
    readonly title: string
    readonly done: number
}

gateway.observe("app", "SELECT * FROM todos WHERE done = ?", [0])
    .pipe(
        filter(isFulfilled),
        map(message => zipSqliteResults<Todo>(message.value)),
    )
    .subscribe(todos => console.log("Open todos:", todos))

// Writing to `todos` makes the query above run again.
await gateway.query("app", "main", "INSERT INTO todos (title) VALUES (?)", ["Buy milk"])
```

## Concepts

### Databases

The first argument to `query` and `observe` is a database name. The first time a name is used, the gateway opens (or creates) the file `<name>-v1.sqlite` in OPFS and keeps it open for the gateway's lifetime. Different names are completely separate databases.

### Results

A query string can contain several statements separated by `;`. `query` resolves to a `SqliteResult` for each statement:

```ts
interface SqliteResult {
    readonly columns: readonly string[]
    readonly rows: readonly (readonly SqliteValue[])[]
}
```

Use `zipSqliteResults<T>(results)` to turn the **last** statement's rows into objects keyed by column name. The type parameter isn't checked, so make sure it matches your columns.

### Parameters

Bind parameters with `?` placeholders and an array of values. A parameter must be a `string`, `number`, `bigint`, `Uint8Array` or `null`. Anything else, including booleans and `undefined`, makes the query reject. Convert booleans to `0`/`1` yourself.

If the SQL string contains several statements, the same parameter array is bound to each of them.

### Connection IDs and transactions

The second argument to `query` is a **connection ID**, a name you choose for a logical session. Queries run one at a time in the order they were queued, with one exception:

- When a query opens a transaction (for example `BEGIN`), its connection ID owns the database until a later query from the **same** ID ends it (`COMMIT` or `ROLLBACK`).
- While a transaction is open, queries from other connection IDs wait in the queue. Only queries from the owning ID run.

```ts
await gateway.query("app", "import", "BEGIN")
try {
    for (const title of titles) {
        await gateway.query("app", "import", "INSERT INTO todos (title) VALUES (?)", [title])
    }
    await gateway.query("app", "import", "COMMIT")
}
catch (error) {
    await gateway.query("app", "import", "ROLLBACK")
    throw error
}
```

Always end the transactions you start. A transaction left open blocks every other connection ID, and every live query, indefinitely.

The ID `"observable"` is used internally by live queries. Don't use it for your own queries.

### Live queries

`observe(db, sql, params?)` returns an Observable of `Message<SqliteResult[]>` values:

| Message | Meaning |
| --- | --- |
| `{ status: "loading" }` | The query is (re)running. |
| `{ status: "fulfilled", value }` | Fresh results. |

Use the type guards `isLoading(message)` and `isFulfilled(message)`, and `mapMessage(message, fn)` to transform the value while keeping the loading state.

How it works:

1. When you subscribe, the gateway runs `EXPLAIN QUERY PLAN` on your SQL to find out which tables it reads.
2. It runs the query, emitting `loading` and then `fulfilled`.
3. SQLite's update hook reports every `INSERT`, `UPDATE` and `DELETE`. When a reported change touches one of the query's tables, the query runs again and emits another `loading`/`fulfilled` pair.

Details:

- Changes are throttled (10 ms) and deduplicated per table, so a burst of writes causes one or two re-runs, not one per row.
- Changes made inside a transaction are held back until the transaction ends, so live queries don't re-run against half-finished data.
- If a change arrives while a re-run is still queued, the stale run is cancelled.
- Changes are also broadcast over a `BroadcastChannel` named `changes-<db>`. Gateways in other tabs or workers on the same origin that have the same database open re-run their live queries too.
- Unsubscribing stops the live query.

A `loading` message is emitted on every re-run. If you only want results, use `filter(isFulfilled)`. To show a spinner only on first load, check whether you already have results.

#### Limitations

Table detection takes the second word of each `EXPLAIN QUERY PLAN` line (for example `SCAN todos` or `SEARCH todos USING INDEX ...`). In practice this means:

- **Don't alias tables in live queries.** For `SELECT * FROM todos AS t`, SQLite reports `SCAN t`, so writes to `todos` are never matched and the query never refreshes.
- Views work, because the plan names the underlying tables.
- Tables that are only reached through foreign key actions or triggers aren't detected from the plan. However, writes made by triggers still show up as changes to the table they write to.
- Only data changes are detected. Schema changes (`ALTER TABLE`, `DROP TABLE`) don't trigger a re-run.

## Using it from the main thread

The gateway has to live in a worker, but its methods return Promises and Observables, so it pairs naturally with [`@ispb/observable-worker`](../observable-worker):

```ts
// db.worker.ts
import { WaSqliteGateway } from "@ispb/observable-sqlite"
import { Channel, expose } from "@ispb/observable-worker"

const gateway = new WaSqliteGateway({})
expose({ target: gateway, channel: Channel.SELF })
```

```ts
// main.ts
import { WaSqliteGateway, isFulfilled, zipSqliteResults } from "@ispb/observable-sqlite"
import { Channel, wrap } from "@ispb/observable-worker"
import { filter, map } from "rxjs"

const db = wrap<WaSqliteGateway>({
    channel: Channel.worker(new URL("./db.worker.js", import.meta.url), { type: "module" }),
}).proxy

await db.query("app", "main", "INSERT INTO todos (title) VALUES (?)", ["Buy milk"])

db.observe("app", "SELECT * FROM todos")
    .pipe(filter(isFulfilled), map(message => zipSqliteResults<Todo>(message.value)))
    .subscribe(render)
```

Unsubscribing in the main thread stops the live query in the worker.

## API reference

### `new WaSqliteGateway(options)`

Pass `{}` if you don't need any options.

- `init?: (api: SQLiteAPI, id: number) => PromiseLike<void>`: called once with the raw wa-sqlite API and database handle each time a database is opened, for example to set pragmas. **Note:** the gateway doesn't currently wait for the returned promise, so queries can start before `init` finishes. Until that's fixed, run schema setup through `gateway.query` instead (as in the quick start).

### `gateway.query(db, connectionId, sql, params?): Promise<readonly SqliteResult[]>`

Runs `sql` and resolves to one result per statement. Rejects if a parameter is invalid or SQLite reports an error. See [Connection IDs and transactions](#connection-ids-and-transactions).

### `gateway.observe(db, sql, params?): Observable<Message<readonly SqliteResult[]>>`

Live query. See [Live queries](#live-queries).

### Messages

- `type Message<T> = { status: "loading" } | { status: "fulfilled", value: T }`
- `isLoading(message)`, `isFulfilled(message)`: type guards.
- `mapMessage(message, fn)`: applies `fn` to a fulfilled message's value. A loading message is returned unchanged.

### Utilities

- `zipSqliteResults<R>(results): R[]`: rows of the last result as objects.
- `sqliteDump(value)`: formats a value as text. `null` becomes `NULL`, booleans become `TRUE`/`FALSE`, and byte arrays become hex.
- `sqliteTest(value)`: whether `value` is an accepted parameter type.
- `BroadcastSubject<T>(key)`: an RxJS `Subject` that also sends every `next` value over a `BroadcastChannel` and emits values received from it. The gateway uses it for cross-tab change notifications.

### Constants

- `SQLITE_OBSERVABLE_DEBOUNCE_TIME` (10): throttle for live query re-runs, in ms.
- `SQLITE_OBSERVABLE_SEND_DEBOUNCE_TIME` (10): throttle for batching outgoing change notifications, in ms.
- `SQLITE_CHANGE_BROADCAST_CHANNEL` (`"changes"`): prefix of the change notification channel name.

## Notes

- **Logging.** Every query and every broadcast change is logged with `console.log`, with timings. Expect a lot of output in the worker's console.
- **No close method.** Opened databases stay open until the worker is terminated.
- **Version suffix.** File names end in `-v1.sqlite`, so a future storage format change can use a new file without clashing with old data.

## Development

```sh
npm install
npm run build    # compile src/ to dist/
npm run watch    # compile on change
npx vite         # run the demo in demo/ on http://127.0.0.1:4785
```

The demo is a page with a text box: type SQL, submit it, and the worker runs it against a database and shows the results.

## License

[MIT](https://choosealicense.com/licenses/mit/)
