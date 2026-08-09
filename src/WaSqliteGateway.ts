

import { Mutex } from 'async-mutex';
import { nanoid } from 'nanoid';
import PLazy from 'p-lazy';
import { equals, flatten, uniq } from 'ramda';
import { BehaviorSubject, buffer, distinctUntilChanged, filter, finalize, from, map, merge, mergeAll, mergeMap, of, startWith, Subject, switchMap, tap, throttleTime } from 'rxjs';
import * as SQLite from 'wa-sqlite';
import { createWaSqliteModule } from './module.js';
import SqliteConnection from './SqliteConnection.js';
import SqliteGateway from './SqliteGateway.js';
import { BroadcastSubject, SQLITE_CHANGE_BROADCAST_CHANNEL, SQLITE_OBSERVABLE_DEBOUNCE_TIME, SQLITE_OBSERVABLE_SEND_DEBOUNCE_TIME, SqliteResult, SqliteResults, sqliteTest, SqliteValue } from './util.js';

export type WaSqliteInit = (api: SQLiteAPI, id: number) => PromiseLike<void>

export interface WaSqliteGatewayOptions {

    /**
     * Initialization callback.
     */
    readonly init?: WaSqliteInit | undefined

}

interface WaSqliteChange {

    readonly operation: number
    readonly db: string
    readonly table: string

}

/*
function openSqliteConnection() {

}

class SqliteConnection {

    constructor(private readonly id: number, private readonly db: string, private readonly api: SQLiteAPI) {

    }

    subject() {
        const subject = new Subject<WaSqliteChange>()
        this.api.update_hook(this.id, (operation, _db, table, _rowid) => {
            if (table === null) {
                return
            }
            subject.next({
                operation,
                db: this.db,
                table,
            })
        })
        //TODO release hook?
        return subject
    }

}
*/

export class WaSqliteGateway implements SqliteGateway<string> {

    private readonly options
    //TODO rename
    private readonly openMutex = new Mutex()
    private readonly module = PLazy.from(createWaSqliteModule)
    private readonly connections = new Map<string, SqliteConnection>()

    constructor(options: WaSqliteGatewayOptions) {
        this.options = options
    }

    private async open(db: string) {
        return await this.openMutex.runExclusive(async () => {
            const module = await this.module
            if (!this.connections.has(db)) {
                const id = await module.api.open_v2(db + "-v1.sqlite")
                this.options.init?.(module.api, id)
                const connection = new WaSqliteConnection(module.api, SQLITE_CHANGE_BROADCAST_CHANNEL + "-" + db, id)
                this.connections.set(db, connection)
            }
            return this.connections.get(db)!
        })
    }

    async query(db: string, connectionId: string, sql: string, params?: readonly unknown[] | undefined) {
        const opened = await this.open(db)
        return opened.query(connectionId, sql, params)
    }
    observe(db: string, sql: string, params?: readonly unknown[] | undefined) {
        const opened = from(this.open(db))
        return opened.pipe(
            switchMap(connection => {
                return connection.observe(sql, params)
            })
        )
    }

}

export class WaSqliteConnection implements SqliteConnection {

    private readonly changes
    private readonly sender = new Subject<WaSqliteChange>()

    constructor(private readonly api: SQLiteAPI, private readonly broadcastKey: string, private readonly id: number) {
        this.changes = new BroadcastSubject<WaSqliteChange>(broadcastKey)
        // this.options = options
        const availability = this.transactionId.pipe(map(id => id === undefined))
        const events = availability.pipe(
            mergeMap(available => {
                if (available) {
                    return this.sender
                }
                else {
                    return this.sender.pipe(
                        buffer(
                            availability.pipe(filter(available => available))
                        )
                    )
                }
            })
        )
        events.pipe(
            buffer(
                events.pipe(throttleTime(SQLITE_OBSERVABLE_SEND_DEBOUNCE_TIME, undefined, { leading: true, trailing: true })),
            ),
            map(flatten),
            map(uniq),
            mergeAll(),
            tap(event => {
                console.log("Sending SQLite change operation.", event)
            })
        ).subscribe(this.changes)
        this.attach()
    }

    private attach() {
        this.api.update_hook(this.id, (operation, _db, table, _rowid) => {
            if (table === null) {
                return
            }
            this.sender.next({
                operation,
                db: this.broadcastKey,
                table,
            })
        })
    }

    private readonly runMutex = new Mutex()
    private async run(sql: string, params?: readonly unknown[] | undefined) {
        params?.forEach(param => {
            if (!sqliteTest(param)) {
                throw new Error("Invalid parameter for this driver: " + param + ".")
            }
        })
        return await this.runMutex.runExclusive(async () => {
            const autoCommitStart = this.api.get_autocommit(this.id)
            const results = new Array<SqliteResult>()
            try {
                for await (const statement of this.api.statements(this.id, sql)) {
                    const rows = new Array<readonly SQLiteCompatibleType[]>()
                    const start = performance.now()
                    this.api.bind_collection(statement, (params ?? []) as SQLiteCompatibleType[])
                    while (await this.api.step(statement) === SQLite.SQLITE_ROW) {
                        rows.push(this.api.row(statement))
                    }
                    const columns = this.api.column_names(statement)
                    const end = performance.now()
                    results.push({
                        rows,
                        columns,
                    })
                    console.log("Executed a SQL query.", { sql, params, results, elapsed: end - start })
                }
            }
            catch (e) {
                console.error("Failed to execute query.", { cause: e, sql, params })
                throw e
            }
            const autoCommit = this.api.get_autocommit(this.id)
            const action = autoCommit !== autoCommitStart ? (autoCommit ? "ended" as const : "started" as const) : undefined
            return {
                results,
                action
            }
        })
    }

    private readonly transactionId = new BehaviorSubject<string | undefined>(undefined)

    private readonly queueMutex = new Mutex()
    private readonly queue = new Array<{ connectionId: string, itemId: string, sql: string, params?: readonly unknown[] | undefined, resolve: (result: SqliteResults) => void, reject: (reason?: unknown) => void }>()
    async query(connectionId: string, sql: string, params?: readonly unknown[] | undefined) {
        const executed = this.add(connectionId, sql, params)
        return await executed.promise
    }

    private add(connectionId: string, sql: string, params?: readonly unknown[] | undefined) {
        const itemId = nanoid()
        return {
            promise: new Promise<SqliteResults>((resolve, reject) => {
                this.queue.push({
                    connectionId,
                    itemId,
                    sql,
                    params,
                    resolve,
                    reject
                })
                this.flush()
            }),
            cancel: () => {
                const found = this.queue.findIndex(_ => _.itemId === itemId)
                if (found !== -1) {
                    this.queue.splice(found, 1)
                }
            },
        }
    }

    flush() {
        this.queueMutex.runExclusive(async () => {
            const found = this.queue.findIndex(_ => _.connectionId === this.transactionId.getValue() || this.transactionId.getValue() === undefined)
            if (found === -1) {
                return
            }
            const item = this.queue.splice(found, 1)[0]!
            try {
                const results = await this.run(item.sql, item.params)
                item.resolve(results.results)
                if (results.action === "started") {
                    this.transactionId.next(item.connectionId)
                }
                if (results.action === "ended") {
                    this.transactionId.next(undefined)
                }
            }
            catch (e) {
                item.reject(e)
            }
            this.flush()
        })
    }

    observe(sql: string, params?: readonly SqliteValue[] | undefined) {
        const explain = this.tablesOf(sql)
        return from(explain)
            .pipe(
                switchMap(tables => {
                    return this.changes.pipe(
                        filter(change => tables.includes(change.table)),
                        throttleTime(SQLITE_OBSERVABLE_DEBOUNCE_TIME, undefined, { leading: true, trailing: true }),
                        startWith(void 0),
                        switchMap(() => {
                            const results = this.add("observable", sql, params)
                            return merge(
                                of({
                                    status: "loading" as const,
                                }),
                                from(results.promise).pipe(
                                    map(value => {
                                        return {
                                            status: "fulfilled" as const,
                                            value,
                                        }
                                    }),
                                    finalize(results.cancel)
                                ),
                            )
                        }),
                        distinctUntilChanged((a, b) => equals(a, b)),
                    )
                })
            )
    }

    /**
     * Figure out which tables a query touches.
     * @param id The database.
     * @param sql The SQL statement.
     * @returns A list of tables.
     */
    private async tablesOf(sql: string) {
        const result = await this.query("observable", "explain query plan " + sql)
        const first = result[0]!
        return first.rows.map(row => {
            const description = row[3] as string
            const table = description.split(" ")[1]!
            return table
        })
    }

}

/*
export default class WaSqliteGateway implements SqliteGateway<string> {

    private readonly module = PLazy.from(createWaSqliteModule)
    private readonly changes = new BroadcastSubject<WaSqliteChange>(SQLITE_CHANGE_BROADCAST_CHANNEL)
    private readonly connections = new Map<string, number>()
    private readonly sender = new Subject<WaSqliteChange>()
    private readonly options

    constructor(options: WaSqliteGatewayOptions = {}) {
        this.options = options
        const availability = this.transactionId.pipe(map(id => id === undefined))
        const events = availability.pipe(
            mergeMap(available => {
                if (available) {
                    return this.sender
                }
                else {
                    return this.sender.pipe(
                        buffer(
                            availability.pipe(filter(available => available))
                        )
                    )
                }
            })
        )
        events.pipe(
            buffer(
                events.pipe(throttleTime(SQLITE_OBSERVABLE_SEND_DEBOUNCE_TIME, undefined, { leading: true, trailing: true })),
            ),
            map(flatten),
            map(uniq),
            mergeAll(),
            tap(event => {
                console.log("Sending SQLite change operation.", event)
            })
        ).subscribe(this.changes)
    }

    private readonly openMutex = new Mutex()

    private async open(db: string) {
        return await this.openMutex.runExclusive(async () => {
            const module = await this.module
            if (!this.connections.has(db)) {
                const id = await module.api.open_v2(db + "-v1.sqlite")
                if (this.options.init !== undefined) {
                    await this.options.init(module.api, id)
                }
                module.api.update_hook(id, (operation, _db, table, _rowid) => {
                    if (table === null) {
                        return
                    }
                    this.sender.next({
                        operation,
                        db,
                        table,
                    })
                })
                this.connections.set(db, id)
            }
            return {
                api: module.api,
                id: this.connections.get(db)!,
            }
        })
    }

    private readonly runMutex = new Mutex()

    private async run(id: string, sql: string, params?: readonly unknown[] | undefined) {
        params?.forEach(param => {
            if (!sqliteTest(param)) {
                throw new Error("Invalid parameter for this driver: " + param + ".")
            }
        })
        const connection = await this.open(id)
        return await this.runMutex.runExclusive(async () => {
            const autoCommitStart = connection.api.get_autocommit(connection.id)
            const results = new Array<SqliteResult>()
            try {
                for await (const statement of connection.api.statements(connection.id, sql)) {
                    const rows = new Array<readonly SQLiteCompatibleType[]>()
                    const start = performance.now()
                    connection.api.bind_collection(statement, (params ?? []) as SQLiteCompatibleType[])
                    while (await connection.api.step(statement) === SQLite.SQLITE_ROW) {
                        rows.push(connection.api.row(statement))
                    }
                    const columns = connection.api.column_names(statement)
                    const end = performance.now()
                    results.push({
                        rows,
                        columns,
                    })
                    console.log("Executed a SQL query.", { sql, params, results, elapsed: end - start })
                }
            }
            catch (e) {
                console.error("Failed to execute query.", { cause: e, sql, params })
                throw e
            }
            const autoCommit = connection.api.get_autocommit(connection.id)
            const action = autoCommit !== autoCommitStart ? (autoCommit ? "ended" as const : "started" as const) : undefined
            return {
                results,
                action
            }
        })
    }

    private readonly transactionId = new BehaviorSubject<string | undefined>(undefined)

    private readonly queueMutex = new Mutex()
    private readonly queue = new Array<{ itemId: string, db: string, connectionId: string, sql: string, params?: readonly unknown[] | undefined, resolve: (result: SqliteResults) => void, reject: (reason?: unknown) => void }>()
    async query(db: string, connectionId: string, sql: string, params?: readonly unknown[] | undefined) {
        const executed = this.add(db, connectionId, sql, params)
        return await executed.promise
    }

    private add(db: string, connectionId: string, sql: string, params?: readonly unknown[] | undefined) {
        const itemId = nanoid()
        return {
            promise: new Promise<SqliteResults>((resolve, reject) => {
                this.queue.push({
                    itemId,
                    db,
                    connectionId,
                    sql,
                    params,
                    resolve,
                    reject
                })
                this.flush()
            }),
            cancel: () => {
                const found = this.queue.findIndex(_ => _.itemId === itemId)
                if (found !== -1) {
                    this.queue.splice(found, 1)
                }
            },
        }
    }

    flush() {
        this.queueMutex.runExclusive(async () => {
            const found = this.queue.findIndex(_ => _.connectionId === this.transactionId.getValue() || this.transactionId.getValue() === undefined)
            if (found === -1) {
                return
            }
            const item = this.queue.splice(found, 1)[0]!
            try {
                const results = await this.run(item.db, item.sql, item.params)
                item.resolve(results.results)
                if (results.action === "started") {
                    this.transactionId.next(item.connectionId)
                }
                if (results.action === "ended") {
                    this.transactionId.next(undefined)
                }
            }
            catch (e) {
                item.reject(e)
            }
            this.flush()
        })
    }

    observe(id: string, sql: string, params?: readonly SqliteValue[] | undefined) {
        const explain = this.tablesOf(id, sql)
        return from(explain)
            .pipe(
                switchMap(tables => {
                    return this.changes.pipe(
                        filter(change => change.db === id),
                        filter(change => tables.includes(change.table)),
                        throttleTime(SQLITE_OBSERVABLE_DEBOUNCE_TIME, undefined, { leading: true, trailing: true }),
                        startWith(void 0),
                        switchMap(() => {
                            const results = this.add(id, "observable", sql, params)
                            return merge(
                                of({
                                    status: "loading" as const,
                                }),
                                from(results.promise).pipe(
                                    map(value => {
                                        return {
                                            status: "fulfilled" as const,
                                            value,
                                        }
                                    }),
                                    finalize(results.cancel)
                                ),
                            )
                        }),
                        distinctUntilChanged((a, b) => equals(a, b)),
                    )
                })
            )
    }

    private async tablesOf(id: string, sql: string) {
        const result = await this.query(id, "observable", "explain query plan " + sql)
        const first = result[0]!
        return first.rows.map(row => {
            const description = row[3] as string
            const table = description.split(" ")[1]!
            return table
        })
    }

}
*/