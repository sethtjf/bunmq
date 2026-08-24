type Handler = (...args: unknown[]) => void;

export class Emitter {
  private handlers = new Map<string, Set<Handler>>();

  on(event: string, fn: Handler): () => void {
    let set = this.handlers.get(event);
    if (!set) {
      set = new Set();
      this.handlers.set(event, set);
    }
    set.add(fn);
    return () => this.off(event, fn);
  }

  off(event: string, fn: Handler): void {
    this.handlers.get(event)?.delete(fn);
  }

  emit(event: string, ...args: unknown[]): void {
    const set = this.handlers.get(event);
    if (set) for (const fn of set) fn(...args);
    const any = this.handlers.get("*");
    if (any) for (const fn of any) fn(event, ...args);
  }

  removeAllListeners(): void {
    this.handlers.clear();
  }
}
