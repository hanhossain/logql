# logql

TODO:
- support reading a directory instead of a single file
- add schema setting for datetime display type (utc or local)
- add `GROUP BY` and `HAVING` support
- support nested queries
- support more tables in the schema (each with different regexes)

Supported features:
- where
  - column compared to literal (and literal compared to column)
    - equals `=`
    - not equals `!=` and `<>`
    - less than `<`
    - greater than `>`
    - less than or equal to `>=`
    - greater than or equal to `>=`
  - multiple clauses
    - and
    - or
    - nested (parentheses)
  - multiline compares against a value's lines combined
- order by
  - ascending `asc`
  - descending `desc`
  - multiple columns `order by last_name, first_name, age desc`
- limit
- offset
- select
  - unnamed expression `select col1, col2`
  - wildcard `select *`,
  - expression with alias `select col1 as cool_alias`
