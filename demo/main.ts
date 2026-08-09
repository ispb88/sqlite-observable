
const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
document.getElementsByTagName("button")[0]!.addEventListener("click", async () => {
    worker.postMessage({
        db: "test",
        query: document.getElementsByTagName("textarea").item(0)?.value
    });
});
worker.onmessage = (event: MessageEvent) => {
    document.getElementsByTagName("div")[0].innerText = JSON.stringify(event.data)
}