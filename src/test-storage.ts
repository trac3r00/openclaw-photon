import type { StorageFilesystem } from "./storage.js";

export class MemoryFilesystem implements StorageFilesystem {
  readonly files = new Map<string, string>();
  readonly modes = new Map<string, number>();
  readonly directories: string[] = [];

  async chmod(path: string, mode: number): Promise<void> {
    this.modes.set(path, mode);
  }

  async mkdir(path: string): Promise<void> {
    this.directories.push(path);
  }

  async readFile(path: string): Promise<string> {
    const value = this.files.get(path);
    if (value === undefined) {
      const error = new Error("not found");
      Object.defineProperty(error, "code", { value: "ENOENT" });
      throw error;
    }
    return value;
  }

  async rename(from: string, to: string): Promise<void> {
    const value = await this.readFile(from);
    this.files.set(to, value);
    this.files.delete(from);
    const mode = this.modes.get(from);
    if (mode !== undefined) {
      this.modes.set(to, mode);
      this.modes.delete(from);
    }
  }

  async unlink(path: string): Promise<void> {
    this.files.delete(path);
  }

  async writeFile(
    path: string,
    data: string,
    options: { flag: "wx"; mode: number },
  ): Promise<void> {
    if (this.files.has(path)) {
      throw new Error("file exists");
    }
    this.files.set(path, data);
    this.modes.set(path, options.mode);
  }
}
