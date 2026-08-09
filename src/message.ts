
interface LoadingMessage {

    readonly status: "loading"

}

export type Message<T> = LoadingMessage | PromiseFulfilledResult<T> //{ readonly status: "fulfilled", value: T } | { readonly status: "loading" }

export function isLoading<T>(message: Message<T>): message is LoadingMessage {
    return message.status === "loading"
}
export function isFulfilled<T>(message: Message<T>): message is PromiseFulfilledResult<T> {
    return message.status === "fulfilled"
}

export function mapMessage<T, O>(message: Message<T>, func: (value: T) => O): Message<O> {
    if (isFulfilled(message)) {
        return {
            ...message,
            value: func(message.value),
        }
    }
    return message
}
