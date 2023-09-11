import { Inspector } from "./inspector";
import { ITableRepository } from "./types";
import * as td from "testdouble";

describe("Inspector", () => {
  it("converts pg types to js equivalents", async () => {
    const repo = td.object<ITableRepository>();
    td.when(repo.getTables("public", ["territory_soc"])).thenResolve([
      {
        name: "test_table",
        namespace: "public",
        type: "table",
        columns: [
          {
            name: "test_bool_column",
            pgType: "bool",
            nullable: false,
          },
          {
            name: "test_date_column",
            pgType: "date",
            nullable: false,
          },
          {
            name: "test_geography_column",
            pgType: "geography",
            nullable: false,
          },
          {
            name: "test_int2_column",
            pgType: "int2",
            nullable: false,
          },
          {
            name: "test_int4_column",
            pgType: "int4",
            nullable: false,
          },
          {
            name: "test_int8_column",
            pgType: "int4",
            nullable: false,
          },
          {
            name: "test_jsonb_column",
            pgType: "jsonb",
            nullable: false,
          },
          {
            name: "test_numeric_column",
            pgType: "numeric",
            nullable: false,
          },
          {
            name: "test_text_column",
            pgType: "text",
            nullable: false,
          },
          {
            name: "test_timestamp_column",
            pgType: "timestamp",
            nullable: false,
          },
          {
            name: "test_timestamptz_column",
            pgType: "timestamptz",
            nullable: false,
          },
          {
            name: "test_uuid_column",
            pgType: "uuid",
            nullable: false,
          },
        ],
      },
    ]);
    const subject = new Inspector(repo);
    const result = await subject.inspect(["territory_soc"]);
    expect(result).toEqual([
      {
        name: "test_table",
        namespace: "public",
        columns: [
          {
            name: "test_bool_column",
            type: "boolean",
          },
          {
            name: "test_date_column",
            type: "Date",
          },
          {
            name: "test_geography_column",
            type: "string",
          },
          {
            name: "test_int2_column",
            type: "number",
          },
          {
            name: "test_int4_column",
            type: "number",
          },
          {
            name: "test_int8_column",
            type: "number",
          },
          {
            name: "test_jsonb_column",
            type: "unknown",
          },
          {
            name: "test_numeric_column",
            type: "number",
          },
          {
            name: "test_text_column",
            type: "string",
          },
          {
            name: "test_timestamp_column",
            type: "Date",
          },
          {
            name: "test_timestamptz_column",
            type: "Date",
          },
          {
            name: "test_uuid_column",
            type: "string",
          },
        ],
      },
    ]);
  });

  it("converts nullable pg types to js equivalents for tables", async () => {
    const repo = td.object<ITableRepository>();
    td.when(repo.getTables("public", ["territory_soc"])).thenResolve([
      {
        name: "test_table",
        namespace: "public",
        type: "table",
        columns: [
          {
            name: "test_bool_column",
            pgType: "bool",
            nullable: true,
          },
        ],
      },
    ]);
    const subject = new Inspector(repo);
    const result = await subject.inspect(["territory_soc"]);
    expect(result).toEqual([
      {
        name: "test_table",
        namespace: "public",
        columns: [
          {
            name: "test_bool_column",
            type: "boolean | null",
          },
        ],
      },
    ]);
  });

  it("converts nullable pg types to non-nullable js equivalents for views", async () => {
    const repo = td.object<ITableRepository>();
    td.when(repo.getTables("public", ["territory_soc"])).thenResolve([
      {
        name: "test_table",
        namespace: "public",
        type: "view",
        columns: [
          {
            name: "test_bool_column",
            pgType: "bool",
            nullable: true,
          },
        ],
      },
    ]);
    const subject = new Inspector(repo);
    const result = await subject.inspect(["territory_soc"]);
    expect(result).toEqual([
      {
        name: "test_table",
        namespace: "public",
        columns: [
          {
            name: "test_bool_column",
            type: "boolean",
          },
        ],
      },
    ]);
  });

  it("converts nullable pg types to non-nullable js equivalents for materialized views", async () => {
    const repo = td.object<ITableRepository>();
    td.when(repo.getTables("public", ["territory_soc"])).thenResolve([
      {
        name: "test_table",
        namespace: "public",
        type: "materialized_view",
        columns: [
          {
            name: "test_bool_column",
            pgType: "bool",
            nullable: true,
          },
        ],
      },
    ]);
    const subject = new Inspector(repo);
    const result = await subject.inspect(["territory_soc"]);
    expect(result).toEqual([
      {
        name: "test_table",
        namespace: "public",
        columns: [
          {
            name: "test_bool_column",
            type: "boolean",
          },
        ],
      },
    ]);
  });
});
