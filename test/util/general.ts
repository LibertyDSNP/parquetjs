/**
 * Generate a randomized hex string of a specific length
 * @param length The length of the hex string
 */
export const generateHexString = (length: number): string => {
    return (
        [...Array(length)]
            .map(() => Math.floor(Math.random() * 16).toString(16))
            .join("")
    );
};

/**
 * Returns a random thing sampled from a list of things
 * @param data - the array of things to sample from
 */
export const sample = <Type>(data: Type[]): Type => {
    const sampleIdx = randInt(data.length - 1)
    return data[sampleIdx]
}

/**
 * Returns a number, randomly chosen, 0 <= n < max
 * @param max: max - 1 is the highest number to generate
 */
export const randInt = (max: number): number => {
    return Math.floor(Math.random() * Math.floor(max));
};

/**
 * Returns a list of N <Type> things, using the provided function.
 * @param n - how many times to do it.
 * @param fn - a function that returns <Type>
 */
export const makeListN = <Type>(n: number, fn: { (): Type }): Type[] => {
    let myThing: any[] = [];
    times(n, () => {
        myThing.push(fn())
    })
    return myThing;
}

/**
 * Do a thing n times, using the provided function.
 * @param n - how many times to do it.
 * @param fn - the thing to do
 */
export const times = (n: number, fn: { (): void }) => {
    for (let i = 0; i < n; i++) {
        fn()
    }
}
