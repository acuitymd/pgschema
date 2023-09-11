import { Printer } from "./printer";
import { Table } from "./types";

describe("Printer", () => {
  it("should print kysely db interface from tables", async () => {
    const subject = new Printer();
    const tables: Table[] = [
      {
        name: "table_one",
        namespace: "public",
        columns: [
          { name: "column_one", type: "string" },
          { name: "column_two", type: "number" },
        ],
      },
      {
        name: "table_two",
        namespace: "other",
        columns: [
          { name: "column_one", type: "Date" },
          { name: "column_two", type: "boolean | null" },
        ],
      },
    ];

    const result = subject.print(tables);
    expect(result)
      .toEqual(`// This is a generated file. Manual changes will be overwritten.

export interface TableOne {
  column_one: string;
  column_two: number;
}

export interface TableTwo {
  column_one: Date;
  column_two: boolean | null;
}

export interface DB {
  table_one: TableOne;
  "other.table_two": TableTwo;
}`);
  });
});
