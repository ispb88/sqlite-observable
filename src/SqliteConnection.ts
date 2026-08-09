import { Observable } from "rxjs"
import { Message } from "./message.js"
import { SqliteResult } from "./util.js"

export default interface SqliteConnection {

    query(connectionId: string, sql: string, params?: readonly unknown[] | undefined): PromiseLike<readonly SqliteResult[]>
    observe(sql: string, params?: readonly unknown[] | undefined): Observable<Message<readonly SqliteResult[]>>

}
