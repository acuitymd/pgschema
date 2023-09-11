import { Pool } from "pg";
import { DB, IDatabase, ITransaction } from "./db";
import { TableRepository } from "./table_repository";

const connectionString = "postgres://test:test@localhost:2345/test";

describe("TableRepository", () => {
  let _pool: Pool;
  let _db: IDatabase;
  let _tx: ITransaction;

  beforeAll(() => {
    _pool = new Pool({ connectionString });
    _db = new DB(_pool);
  });

  afterAll(() => _pool.end());

  afterEach(() => _tx.rollback());

  beforeEach(async () => {
    _tx = await _db.beginTx();
  });

  it("works for tables", async () => {
    _tx.query(`
      CREATE TABLE "public"."territory_soc" (
          "id" serial PRIMARY KEY,
          "soc_id" text NOT NULL,
          "is_included" bool NOT NULL,
          "test_nullable" text
      );
    `);
    const subject = new TableRepository(_tx);
    const result = await subject.getTables("public", ["territory_soc"]);
    expect(result).toEqual([
      {
        name: "territory_soc",
        namespace: "public",
        type: "table",
        columns: [
          {
            name: "id",
            pgType: "int4",
            nullable: false,
          },
          {
            name: "soc_id",
            pgType: "text",
            nullable: false,
          },
          {
            name: "is_included",
            pgType: "bool",
            nullable: false,
          },
          {
            name: "test_nullable",
            pgType: "text",
            nullable: true,
          },
        ],
      },
    ]);
  });

  it("works for views", async () => {
    _tx.query(`
      CREATE TABLE "public"."territory_soc" (
          "id" serial PRIMARY KEY,
          "soc_id" text NOT NULL,
          "is_included" bool NOT NULL,
          "test_nullable" text
      );
    `);
    _tx.query(`
      CREATE VIEW "public"."territory_soc_view" AS
      SELECT
          "territory_soc"."id",
          "territory_soc"."soc_id",
          "territory_soc"."is_included",
          "territory_soc"."test_nullable"
      FROM "public"."territory_soc";
    `);
    const subject = new TableRepository(_tx);
    const result = await subject.getTables("public", ["territory_soc_view"]);
    expect(result).toEqual([
      {
        name: "territory_soc_view",
        namespace: "public",
        type: "view",
        columns: [
          {
            name: "id",
            pgType: "int4",
            nullable: true,
          },
          {
            name: "soc_id",
            pgType: "text",
            nullable: true,
          },
          {
            name: "is_included",
            pgType: "bool",
            nullable: true,
          },
          {
            name: "test_nullable",
            pgType: "text",
            nullable: true,
          },
        ],
      },
    ]);
  });

  it("works for materialized views", async () => {
    _tx.query(`
    CREATE TABLE "public"."territory_soc" (
        "id" serial PRIMARY KEY,
        "soc_id" text NOT NULL,
        "is_included" bool NOT NULL,
        "test_nullable" text
    );`);

    _tx.query(`
    CREATE MATERIALIZED VIEW "public"."territory_soc_materialized_view" AS
    SELECT
        "territory_soc"."id",
        "territory_soc"."soc_id",
        "territory_soc"."is_included",
        "territory_soc"."test_nullable"
    FROM "public"."territory_soc";`);

    const subject = new TableRepository(_tx);
    const result = await subject.getTables("public", [
      "territory_soc_materialized_view",
    ]);
    expect(result).toEqual([
      {
        name: "territory_soc_materialized_view",
        namespace: "public",
        type: "materialized_view",
        columns: [
          {
            name: "id",
            pgType: "int4",
            nullable: true,
          },
          {
            name: "soc_id",
            pgType: "text",
            nullable: true,
          },
          {
            name: "is_included",
            pgType: "bool",
            nullable: true,
          },
          {
            name: "test_nullable",
            pgType: "text",
            nullable: true,
          },
        ],
      },
    ]);
  });

  it("throws when a requested table does not exist", async () => {
    const subject = new TableRepository(_tx);
    await expect(
      subject.getTables("public", ["territory_soc"]),
    ).rejects.toThrow(/Missing tables: territory_soc/);
  });

  it("supports namespaces other than public", async () => {
    _tx.query(`
      CREATE SCHEMA "other";
    `);
    _tx.query(`
      CREATE TABLE "other"."territory_soc" (
          "id" serial PRIMARY KEY,
          "soc_id" text NOT NULL,
          "is_included" bool NOT NULL,
          "test_nullable" text
      );
    `);
    const subject = new TableRepository(_tx);
    const result = await subject.getTables("other", ["territory_soc"]);
    expect(result).toEqual([
      {
        name: "territory_soc",
        namespace: "other",
        type: "table",
        columns: [
          {
            name: "id",
            pgType: "int4",
            nullable: false,
          },
          {
            name: "soc_id",
            pgType: "text",
            nullable: false,
          },
          {
            name: "is_included",
            pgType: "bool",
            nullable: false,
          },
          {
            name: "test_nullable",
            pgType: "text",
            nullable: true,
          },
        ],
      },
    ]);
  });
});
