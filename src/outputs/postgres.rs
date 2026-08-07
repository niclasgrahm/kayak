use anyhow::{Context, Result, anyhow, bail};
use kayak_core::config::PostgresOutputConfig;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing::error;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    secrets::Resolved,
};

const DEFAULT_PORT: u16 = 5432;

impl BuildOutput for PostgresOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        // rejected at build time rather than on first insert: a bad table name
        // should fail the pipeline that owns it, not surface an hour later
        let table = Table::parse(&self.table)?;
        Ok(Box::new(PostgresOutput {
            host: self.host,
            port: self.port.unwrap_or(DEFAULT_PORT),
            database: self.database,
            user: self.user,
            password: ctx
                .resolve(&self.password)
                .context("failed to resolve secrets in the postgres output password")?,
            table,
            client: None,
        }))
    }
}

/// A validated table name, quoted for interpolation into a statement.
///
/// The table cannot be a bind parameter — postgres only takes those where a
/// *value* goes — so it ends up in the SQL text, and anything reaching the SQL
/// text from config has to be checked first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    quoted: String,
}

impl Table {
    /// Accepts `readings` or `analytics.readings`; each part must look like an
    /// unquoted postgres identifier. Deliberately stricter than postgres itself
    /// — a table named `"drop table"` is legal there and not worth supporting.
    pub fn parse(name: &str) -> Result<Self> {
        let parts: Vec<&str> = name.split('.').collect();
        if parts.len() > 2 {
            bail!("invalid postgres table name '{name}': expected 'table' or 'schema.table'");
        }
        for part in &parts {
            if part.is_empty() {
                bail!("invalid postgres table name '{name}': it has an empty part");
            }
            if !part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                bail!(
                    "invalid postgres table name '{name}': only letters, digits and underscores are allowed"
                );
            }
            if part.starts_with(|c: char| c.is_ascii_digit()) {
                bail!("invalid postgres table name '{name}': a part cannot start with a digit");
            }
        }
        // quoted so that a name colliding with a keyword still works, and
        // because the identifier is case-sensitive once quoted — which is what
        // makes what the config says the name of the table that appears
        let quoted = parts
            .iter()
            .map(|p| format!("\"{p}\""))
            .collect::<Vec<_>>()
            .join(".");
        Ok(Self { quoted })
    }

    #[must_use]
    pub fn create_table_sql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {table} (\
             id BIGSERIAL PRIMARY KEY, \
             received_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
             payload JSONB NOT NULL)",
            table = self.quoted
        )
    }

    #[must_use]
    pub fn insert_sql(&self) -> String {
        format!(
            "INSERT INTO {table} (payload) VALUES ($1)",
            table = self.quoted
        )
    }
}

pub struct PostgresOutput {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: Resolved,
    table: Table,
    client: Option<tokio_postgres::Client>,
}

impl PostgresOutput {
    /// Built field by field rather than as a url, so a password containing `@`
    /// or `/` needs no escaping — and so the password never has to be
    /// concatenated into a string that might get logged.
    fn connection_config(&self) -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(self.password.expose());
        config
    }

    /// How this connection is described in an error. Everything but the
    /// password, which `Resolved` would not print anyway.
    fn describe(&self) -> String {
        format!(
            "postgres://{user}@{host}:{port}/{database}",
            user = self.user,
            host = self.host,
            port = self.port,
            database = self.database
        )
    }
}

#[async_trait::async_trait]
impl OutputDestination for PostgresOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        // as in the nats output: doing nothing here would look like the rows
        // were written
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("postgres output is not connected; init() was not called"))?;

        let statement = client
            .prepare(&self.table.insert_sql())
            .await
            .with_context(|| format!("failed to prepare the insert for {}", self.describe()))?;

        for msg in message_batch.iter() {
            client
                .execute(&statement, &[&**msg])
                .await
                .with_context(|| format!("failed to insert a row into {}", self.describe()))?;
        }
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let (client, connection) = self
            .connection_config()
            .connect(NoTls)
            .await
            .with_context(|| format!("failed to connect to {}", self.describe()))?;

        // the connection drives the socket and has to be polled for the client
        // to work at all; it resolves when the client is dropped or the server
        // goes away, and the error it returns then surfaces on the next insert
        let described = self.describe();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres connection to {described} closed: {e:?}");
            }
        });

        client
            .execute(&self.table.create_table_sql(), &[])
            .await
            .with_context(|| format!("failed to create the table in {}", self.describe()))?;

        self.client = Some(client);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Table;

    #[test]
    fn a_plain_table_name_is_quoted() -> anyhow::Result<()> {
        let table = Table::parse("readings")?;
        assert_eq!(
            table.insert_sql(),
            r#"INSERT INTO "readings" (payload) VALUES ($1)"#
        );
        assert!(
            table
                .create_table_sql()
                .starts_with(r#"CREATE TABLE IF NOT EXISTS "readings" ("#),
            "{}",
            table.create_table_sql()
        );
        Ok(())
    }

    #[test]
    fn a_schema_qualified_name_quotes_each_part_separately() -> anyhow::Result<()> {
        assert_eq!(
            Table::parse("analytics.readings")?.insert_sql(),
            r#"INSERT INTO "analytics"."readings" (payload) VALUES ($1)"#
        );
        Ok(())
    }

    /// The table name is interpolated into the SQL text, so this check is the
    /// only thing between `config.json` and arbitrary statements.
    #[test]
    fn a_name_that_could_carry_sql_is_rejected() {
        for name in [
            r#"readings"; drop table users; --"#,
            "readings; drop table users",
            "readings users",
            "readings--",
            "a.b.c",
            "",
            "analytics.",
            "1readings",
            "readings'",
        ] {
            assert!(
                Table::parse(name).is_err(),
                "'{name}' should have been rejected"
            );
        }
    }

    /// The payload column is the whole message, and the row's identity and
    /// arrival time are the table's own business — a config with no `id` field
    /// must still produce insertable rows.
    #[test]
    fn the_insert_only_ever_binds_the_payload() -> anyhow::Result<()> {
        let sql = Table::parse("readings")?.insert_sql();
        assert!(sql.contains("(payload)"), "{sql}");
        assert_eq!(sql.matches('$').count(), 1, "{sql}");
        Ok(())
    }
}
