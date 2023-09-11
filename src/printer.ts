import { Table } from "./types";

export class Printer {
  constructor() {}

  print(tables: Table[]): string {
    const warning = `// This is a generated file. Manual changes will be overwritten.`;

    const tableInterfaces = tables.map((table) => {
      return `export interface ${snakeToPascalCase(table.name)} {
${table.columns.map((column) => `  ${column.name}: ${column.type};`).join("\n")}
}`;
    });

    const dbInterface = `export interface DB {
${tables
  .map((table) => {
    const tableName =
      table.namespace === "public"
        ? table.name
        : `"${table.namespace}.${table.name}"`;
    return `  ${tableName}: ${snakeToPascalCase(table.name)};`;
  })
  .join("\n")}
}`;

    return [warning, ...tableInterfaces, dbInterface].join("\n\n");
  }
}

function snakeToPascalCase(snake: string): string {
  return snake
    .split("_")
    .map((word) => word[0].toUpperCase() + word.slice(1))
    .join("");
}
