# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Maven multi-module catalog of Hazelcast code samples. The root `pom.xml` aggregates all top-level sample folders and pins shared versions: **Hazelcast 5.5.0** and **Java 17**. Each top-level folder is an independent sample area with its own `pom.xml` and usually a `README.md`.

## Build Commands

```bash
# Build the entire repo
mvn install

# Build and test a single module (from repo root)
mvn -pl <module-path> -am install

# Run tests for a single module
mvn -pl <module-path> -am test

# Run checkstyle validation
mvn checkstyle:check

# Run a compiled sample directly (if no script is provided)
java -cp target/classes:target/lib/* <fully.qualified.MainClass>
```

## Running Samples

1. **Check the module's `README.md` first** — it describes the required startup order, number of terminals, ports, and cluster size.
2. **Use provided shell scripts** when present (`start.sh`, `start-member.sh`, `start-client.sh`). These are generated into `<module>/bin/` during `install` for the `demo` module.
3. **Fall back to direct `java` invocation** only when no script exists.

Key multi-process samples:
- **`cp-subsystem`** — CP data-structure demos require 3 terminal instances running the same `start.sh` to form a CP cluster.
- **`sql/hazdb`** — Full client-server demo; build with `mvn clean install dockerfile:build`, then run via Docker scripts in `src/main/scripts/`.
- **`clients/`** — Separate member (`start-member.sh`) and client (`start-client.sh`) processes.

## Code Conventions

- **Java 17** source compatibility is required.
- **Apache license header** required on all public classes — use the template in `checkstyle/ClassHeader.txt`.
- **Checkstyle rules** are in `checkstyle/checkstyle.xml`. Key restrictions:
  - Max line length: 130 characters
  - No star imports, no `sun.*` packages, no `org.jetbrains.annotations`
  - No trailing spaces or tabs for indentation
- **Package naming**: `com.hazelcast.samples.<area>` for most modules; some standalone demos use `example.*` or default-package launcher classes.

## Architecture

```
/
├── helper/          # Shared utilities and LicenseUtils for Enterprise samples
├── checkstyle/      # Checkstyle config and Apache license header template
├── ai/              # AI use cases (e.g., movie-recommendation with vector search)
├── jet/             # Stream/batch processing via Pipeline API
├── sql/             # SQL and JDBC examples; sql/hazdb is a full client-server demo
├── cp-subsystem/    # CP data structures (requires 3-node CP cluster to run)
├── enterprise/      # Enterprise-only features (requires license key via LicenseUtils)
├── hazelcast-integration/  # Integrations: Hibernate 2nd-level cache, Spring, K8s, etc.
├── serialization/   # DataSerializable, Portable, custom serializers (Kryo, Protobuf)
└── <feature>/       # One folder per Hazelcast feature area
```

### Key structural patterns

- **Role-split processes**: Many samples are intentionally split into member/client, publisher/subscriber, or server/client roles. Keep these boundaries intact.
- **Helper module**: `helper/` contains shared utilities including `LicenseUtils` — required by all `enterprise/` samples. Build `helper` before running enterprise samples (`mvn -pl helper install`).
- **Module-local tooling**: Some modules add specialized Maven plugins — `sql/hazdb` uses `dockerfile-maven-plugin` and `frontend-maven-plugin`; `jet` modules may need Avro/Protobuf code generation plugins.

### Enterprise samples

Enterprise features require a Hazelcast Enterprise trial or commercial license. See `helper/README.md` and `LicenseUtils` for how to configure the license key before running samples under `enterprise/`.
