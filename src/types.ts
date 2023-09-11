export type Column = {
  name: string;
  type: string;
};

export type Table = {
  name: string;
  namespace: string;
  columns: Column[];
};

export type ColumnData = {
  name: string;
  pgType: string;
  nullable: boolean;
};

export type TableData = {
  name: string;
  namespace: string;
  type: "table" | "view" | "materialized_view";
  columns: ColumnData[];
};

export interface ITableRepository {
  getTables(namespace: string, tables: string[]): Promise<TableData[]>;
}
