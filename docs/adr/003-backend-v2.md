# 003 Backend V2

Status: Accepted

Date: 2025-04-29

Tags: driver, backend, architecture

## Context

To reap benefits from further changes (index, query, schema, new ttl and watcher) to the backend we need
to break the schema without breaking existing clusters. A new backend will include a new driver (dqlite, sqlite)
and an internal backend and will be used for new clusters only (checked via sqlite metadata user_version).
The old driver will continue to exist to support old clusters. In the future we will implement a manual
migration path to migrate old clusters to the new backend.

## Decision

The new backend will be implemented as a new driver (dqlite, sqlite) and an internal backend. The old driver
will continue to exist to support old clusters. As a second step we will implement a manual migration path
to migrate old clusters to the new backend. The new backend will be used for new clusters only.

## Consequences

**Positive:**
* The new backend will allow us to implement new features and improvements without breaking existing clusters.
* The new driver will be more efficient and reliable than the old driver.
* The new backend will be easier to maintain and extend in the future.
* The new backend allows us to experiment with new features and improvements without affecting existing clusters.

**Negative:**
* Exisiting clusters will not benefit from the new features and improvements until they are migrated to the new backend.
* The new backend will require additional testing and validation to ensure compatibility with existing clusters.
* The new backend some duplication of code and functionality with the old driver, which may lead to increased maintenance overhead.

## Considered Options

* **Option 1: Keep the existing backend and driver.**
  * **Pros:** No changes required, no risk of breaking existing clusters.
  * **Cons:** Limited ability to implement new features and improvements, potential performance issues.
* **Option 2: Implement a new backend and driver.**
  * **Pros:** Allows for new features and improvements, more efficient and reliable, easier to maintain.
  * **Cons:** Requires migration of existing clusters, potential duplication of code and functionality.
* **Option 3: Implement a new backend and driver with a manual migration path.**
  * **Pros:** Allows for new features and improvements, more efficient and reliable, easier to maintain, provides a path for existing clusters to migrate. Gives users the option to migrate at their own pace.
  * **Cons:** Requires additional testing and validation, potential duplication of code and functionality.

## Links

* [Backend V2 PR](https://github.com/canonical/k8s-dqlite/pull/279)
