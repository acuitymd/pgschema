import { IDatabase } from "./db";
import { ITableRepository, TableData } from "../types";
import { groupBy } from "lodash";

type _Row = {
  table_name: string;
  column_name: string;
  data_type: string;
  not_null: boolean;
  relkind: "r" | "v" | "m";
};

export class TableRepository implements ITableRepository {
  constructor(private db: IDatabase) {}

  async getTables(namespace: string, tables: string[]): Promise<TableData[]> {
    const q = `
      SELECT
          c.relname AS table_name,
          a.attname AS column_name,
          t.typname AS data_type,
          a.attnotnull AS not_null,
          c.relkind
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
      JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
      WHERE
          c.relname = ANY($1)
          AND n.nspname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
      ORDER BY c.relname, a.attnum;
    `;
    const { rows } = await this.db.query<_Row>(q, [tables, namespace]);
    const tablesArray = this.parseTables(namespace, rows);
    assertAllTablesArePresent(tables, tablesArray);
    return tablesArray;
  }

  private parseTables(namespace: string, rows: _Row[]): TableData[] {
    const byTable = groupBy(rows, (row) => row.table_name);
    const tablesArray = Object.entries(byTable).map(([name, rows]) => {
      return {
        name,
        namespace,
        type: getTableType(rows[0].relkind),
        columns: rows.map((row) => ({
          name: row.column_name,
          pgType: row.data_type,
          nullable: !row.not_null,
        })),
      };
    });

    return tablesArray;
  }
}

// we want to fail if a table that's specified in the config is not present in
// the database, since this would mean that the config is out of sync with the
// database
function assertAllTablesArePresent(tables: string[], tablesArray: TableData[]) {
  const tablesArrayNames = tablesArray.map((t) => t.name);
  const missingTables = tables.filter((t) => !tablesArrayNames.includes(t));
  if (missingTables.length > 0) {
    throw new Error(`Missing tables: ${missingTables.join(", ")}`);
  }
}

function getTableType(relkind: string): "table" | "view" | "materialized_view" {
  switch (relkind) {
    case "r":
      return "table";
    case "v":
      return "view";
    case "m":
      return "materialized_view";
    default:
      throw new Error(`Unknown relkind: ${relkind}`);
  }
}
