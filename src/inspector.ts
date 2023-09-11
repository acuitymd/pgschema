import { ITableRepository, Table, TableData } from "./types";

export class Inspector {
  constructor(private tr: ITableRepository) {}

  async inspect(tables: string[]): Promise<Table[]> {
    const namespaceToTablesMap = parseTableNames(tables);
    const promises: Promise<TableData[]>[] = [];
    for (const [namespace, tables] of Object.entries(namespaceToTablesMap)) {
      promises.push(this.tr.getTables(namespace, tables));
    }
    const result = await Promise.all(promises).then((r) => r.flat());
    return result.map((table) => ({
      name: table.name,
      namespace: table.namespace,
      columns: table.columns.map((column) => ({
        name: column.name,
        type: getJSType(
          column.pgType,
          getNullable(column.nullable, table.type),
        ),
      })),
    }));
  }
}

function parseTableNames(tables: string[]): Record<string, string[]> {
  return tables.reduce(
    (acc, table) => {
      const parts = table.split(".");
      if (parts.length > 2) {
        throw new Error(`Invalid table name: ${table}`);
      }
      if (parts.length === 1) {
        parts.unshift("public");
      }
      const [namespace, name] = parts;
      const current = acc[namespace] || [];
      current.push(name);
      acc[namespace] = current;
      return acc;
    },
    {} as Record<string, string[]>,
  );
}

/*
 * Types we need to support
 * bool => boolean
 * date => Date
 * geography => string
 * int2 => number
 * int4 => number
 * int8 => number
 * jsonb => unknown
 * numeric => number
 * text => string
 * timestamp => Date
 * timestamptz => Date
 * uuid => string
 *
 * Query to get all types for a list of tables from a namespace:
 *
 * SELECT
 * t.typname AS data_type
 * FROM pg_catalog.pg_class c
 * JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
 * JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
 * JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
 * WHERE
 * c.relname in ($1)
 * AND n.nspname = $2
 * AND a.attnum > 0
 * AND NOT a.attisdropped
 * GROUP BY data_type;
 */
function getJSType(pgType: string, nullable: boolean): string {
  switch (pgType) {
    case "bool":
      return nullable ? "boolean | null" : "boolean";
    case "date":
    case "timestamp":
    case "timestamptz":
      return nullable ? "Date | null" : "Date";
    case "geography":
    case "text":
    case "uuid":
      return nullable ? "string | null" : "string";
    case "int2":
    case "int4":
    case "int8":
    case "numeric":
      return nullable ? "number | null" : "number";
    case "jsonb":
      return nullable ? "unknown | null" : "unknown";
    default:
      throw new Error(`Unknown type: ${pgType}`);
  }
}

// In pg views and materialized views columns are nullable by default, even if
// the underlying table value is not nullable. Since we don't want to deal with
// optionals everywhere, we make them non-nullable.
function getNullable(nullable: boolean, relkind: string): boolean {
  if (relkind === "table") {
    return nullable;
  }
  return false;
}
