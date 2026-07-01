## PipelineBuilder.load()


Configure database session or client for persistence.


Usage

``` python
PipelineBuilder.load(
    session,
    *,
    upsert=False,
    upsert_on=None,
    batch_size=1000,
)
```


When load() is called before run(), the pipeline will: 1. Map instances by relationship component 2. Bind relationships within each component 3. Add instances to the session (SQLAlchemy) or insert to database (Supabase) 4. Flush and evict each component (instances are not retained in PipelineResult)

The caller controls the transaction (commit/rollback) for SQLAlchemy.


## Parameters


`session: Any`  
SQLAlchemy/SQLModel session or Supabase client.

`upsert: bool = ``False`  
If True, use upsert instead of insert (Supabase only). Uses the table's primary key for conflict detection by default.

`upsert_on: dict[str, str | list[str]] | None = None`  
Override conflict columns per table (Supabase only). Only used when upsert=True. Maps table names to conflict columns:

- Single column: {"users": "email"}
- Composite key: {"posts": \["user_id", "slug"\]}

`batch_size: int = ``1000`  
Maximum rows per insert batch (Supabase only).


## Returns


`PipelineBuilder`  
Self for method chaining.


Example (SQLAlchemy): result = ( etl(data) .goto("users").each() .map_to(table=User, fields=\[…\]) .load(session) .run() ) session.commit() \# Caller controls transaction

Example (Supabase with default conflict): result = ( etl(data) .goto("users").each() .map_to(table="users", fields=\[…\]) .load(supabase_client, upsert=True) .run() )

Example (Supabase with custom conflict columns): result = ( etl(data) .goto("users").each() .map_to(table="users", fields=\[…\]) .load(supabase_client, upsert=True, upsert_on={ "users": "email", "posts": \["user_id", "slug"\], }) .run() )
