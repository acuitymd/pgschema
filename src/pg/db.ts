import { Pool, PoolClient } from "pg";

export type QueryRow = {
  [column: string]: any;
};

export type QueryResult<T extends QueryRow> = {
  rowCount: number;
  rows: T[];
};

export interface IDatabase {
  beginTx: () => Promise<ITransaction>;
  query: <T extends QueryRow>(
    sql: string,
    params?: any[],
  ) => Promise<QueryResult<T>>;
}

export interface ITransaction extends IDatabase {
  commit: () => Promise<void>;
  rollback: () => Promise<void>;
}

export class DB implements IDatabase {
  constructor(private pool: Pool) {}

  async beginTx(): Promise<ITransaction> {
    const client = await this.pool.connect();
    await client.query("BEGIN;");
    return new Transaction(client);
  }

  public async query<T extends QueryRow>(
    sql: string,
    params?: any[],
  ): Promise<QueryResult<T>> {
    return await this.pool.query(sql, params);
  }
}

class Transaction implements ITransaction {
  private child: ITransaction | null = null;
  private closed = false;

  constructor(
    private client: PoolClient,
    private savePoint = 0,
  ) {}

  public async query<T extends QueryRow>(
    sql: string,
    params?: any[],
  ): Promise<QueryResult<T>> {
    return await this.client.query(sql, params);
  }

  public async commit(): Promise<void> {
    if (this.child) {
      await this.child.commit();
    }
    if (this.closed) {
      return;
    }
    if (this.savePoint > 0) {
      await this.client.query(`RELEASE SAVEPOINT ${this.savePointName()};`);
    } else {
      await this.client.query("COMMIT;");
      this.client.release();
    }
    this.closed = true;
  }

  public async rollback(): Promise<void> {
    if (this.child) {
      await this.child.rollback();
    }
    if (this.closed) {
      return;
    }
    if (this.savePoint > 0) {
      await this.client.query(`ROLLBACK TO SAVEPOINT ${this.savePointName()};`);
    } else {
      await this.client.query("ROLLBACK;");
      this.client.release();
    }
    this.closed = true;
  }

  public async beginTx(): Promise<ITransaction> {
    const savePoint = this.savePoint + 1;
    await this.client.query(`SAVEPOINT ${this.savePointName(savePoint)};`);
    this.child = new Transaction(this.client, savePoint);
    return this.child;
  }

  private savePointName(savePoint?: number): string {
    if (savePoint) {
      return `savepoint${savePoint}`;
    }
    return `savepoint${this.savePoint}`;
  }
}
