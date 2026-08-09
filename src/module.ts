

import * as SQLite from 'wa-sqlite';
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite-async.mjs';
//@ts-ignore
import { OPFSAdaptiveVFS } from "wa-sqlite/src/examples/OPFSAdaptiveVFS.js";

export async function createWaSqliteModule() {
    const module = await SQLiteESMFactory()
    const api = SQLite.Factory(module)
    const vfs = await OPFSAdaptiveVFS.create("adaptive", module)
    api.vfs_register(vfs, true)
    return {
        vfs,
        api,
    }
}

export type WaSqliteModule = Awaited<ReturnType<typeof createWaSqliteModule>>
