# mysql-es

> A MySql implementation of the `EventStore` trait in cqrs-es.

---

## Usage
Add to your Cargo.toml file:

```toml
[dependencies]
cqrs-es = "0.2.4"
persist-es = "0.2.4"
mysql-es = "0.2.4"
```

Requires access to a Postgres DB with existing tables. See:
- [Sample database configuration](db/init.sql)
- Use `docker-compose` to quickly setup [a local database](docker-compose.yml)

A simple configuration example:
```
let store = default_mysql_pool("mysql://my_user:my_pass@localhost:3306/my_db");
let cqrs = mysql_es::mysql_cqrs(pool, vec![])
```

Things that could be helpful:
- [User guide](https://doc.rust-cqrs.org) along with an introduction to CQRS and event sourcing.
- [Demo application](https://github.com/serverlesstechnology/cqrs-demo) using the warp http server.
- [Change log](https://github.com/serverlesstechnology/cqrs/blob/master/change_log.md)

[![Crates.io](https://img.shields.io/crates/v/mysql-es)](https://crates.io/crates/mysql-es)
[![docs](https://img.shields.io/badge/API-docs-blue.svg)](https://docs.rs/mysql-es)