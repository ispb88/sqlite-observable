import { zipObj } from "ramda"
import { Subject, fromEvent, map } from "rxjs"

/**
 * Debounce all query observables by default.
 */
export const SQLITE_OBSERVABLE_DEBOUNCE_TIME = 10
export const SQLITE_OBSERVABLE_SEND_DEBOUNCE_TIME = 10

/**
 * The broadcast channel for change notifications.
 */
export const SQLITE_CHANGE_BROADCAST_CHANNEL = "changes"

export type SqliteValue = string | number | bigint | Uint8Array | ArrayBuffer | number[] | null

export interface SqliteResult {

    readonly columns: readonly string[]
    readonly rows: readonly (readonly SqliteValue[])[]

}

export type SqliteResults = readonly SqliteResult[]

/**
 * Dumps a Sqlite value to a string.
 */
export function sqliteDump(value: SqliteValue | boolean) {
    if (typeof value === "boolean") {
        return value ? "TRUE" : "FALSE"
    }
    if (value instanceof Uint8Array) {
        return [...value].map(n => n.toString(16).padStart(2, '0')).join('')
    }
    if (Array.isArray(value)) {
        return value.map(n => n.toString(16).padStart(2, '0')).join('')
    }
    return value?.toString() ?? "NULL"
}

/**
 * Tests if a value is an sqlite value.
 */
export function sqliteTest(value: unknown): value is SqliteValue {
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "bigint" && !(value instanceof Uint8Array) && value !== null) {
        return false
    }
    return true
}

/**
 * Zips results into objects.
 */
export function zipSqliteResults<R>(results: readonly SqliteResult[]) {
    return results.slice(-1).flatMap(result => result.rows.map(zipObj(result.columns))) as unknown as R[]
}

export class BroadcastSubject<T> extends Subject<T> {

    private readonly channel
    private readonly subscription

    constructor(key: string) {
        super()
        this.channel = new BroadcastChannel(key)
        this.subscription = fromEvent<MessageEvent>(this.channel, "message").pipe(map(_ => _.data)).subscribe({ next: super.next.bind(this), error: super.error.bind(this), complete: super.complete.bind(this) })
    }

    override next(value: T) {
        super.next(value)
        this.channel.postMessage(value)
    }
    override complete() {
        super.complete()
        this.subscription.unsubscribe()
        this.channel.close()
    }

}
