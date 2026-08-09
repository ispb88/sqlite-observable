import WaSqliteGateway from "../src/WaSqliteGateway.js";

const gateway = new WaSqliteGateway();
self.onmessage = async (event: MessageEvent) => {
    const command = event.data as { readonly db: string, readonly query: string }
    try {
        const result = await gateway.query("test", command.db, command.query);
        self.postMessage(result);
    }
    catch (error) {
        self.postMessage(`${error}`)
    }
}