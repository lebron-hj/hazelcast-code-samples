# AGENTS.md

## Project shape
- This repo is a Maven multi-module catalog of Hazelcast code samples. The root `pom.xml` aggregates the top-level folders under `modules` and pins shared versions such as Hazelcast `5.5.0` and Java `17`.
- Treat each top-level folder as an independent sample area; many folders contain nested Maven builds and their own `README.md` files.
- Shared utilities live in `helper/`, while feature-specific samples stay inside their feature folder (for example `ai/movie-recommendation`, `sql/hazdb`, `cp-subsystem`, `jet/`).

## Read before editing
- Start with the nearest `README.md` and the module `pom.xml` before changing code. The root README explicitly says to follow sample-local instructions or scripts when they exist.
- Prefer the sample’s documented run path over inventing a new one. Examples: `demo` copies runnable scripts into `demo/bin` during `install`; `cp-subsystem` requires three terminals for CP data-structure demos; `sql/hazdb` builds and runs via Docker and has separate `client`, `server`, `common`, and `management-center` modules.
- `jet/README.md` is a good map of entry classes and use cases; many Jet examples are organized by scenario rather than by API surface.

## Build and test workflow
- Common repo-wide build: `mvn install` from the root.
- For a single sample, use targeted Maven builds from that module, e.g. `mvn -pl <module> -am test` or the module’s documented command.
- Some samples add extra tooling in their own POMs: `sql/hazdb` uses `dockerfile-maven-plugin` and `frontend-maven-plugin`, `demo` uses `maven-resources-plugin` to generate scripts, and `spring`/`sql` samples may pull in Spring Boot or frontend assets.

## Code conventions
- Keep Java 17 source compatible and follow the repo’s Checkstyle rules in `checkstyle/checkstyle.xml`.
- Public classes should include the Apache license header from `checkstyle/ClassHeader.txt`.
- Avoid trailing spaces, tabs, and long lines over the configured 130-character limit.
- Avoid star imports, `sun.*`, and `org.jetbrains.annotations`; these are explicitly restricted by Checkstyle.
- Package names are usually `com.hazelcast.samples...`, but some standalone demos use short local packages such as `example.*` or default-package launcher classes.

## Sample-specific patterns to preserve
- Many samples are intentionally split into role-based processes: member/client, master/slave, publisher/subscriber, or server/client/common. Keep those boundaries intact when editing code or docs.
- When a sample’s README describes startup order, ports, cluster size, or terminal count, keep the code and scripts aligned with that narrative.
- If you change a module’s build or run behavior, update its local `README.md` and any referenced shell scripts at the same time.

## Useful reference points
- Root overview: `README.md`
- Shared rules: `checkstyle/checkstyle.xml` and `checkstyle/ClassHeader.txt`
- Shared helpers: `helper/README.md`
- Complex multi-module example: `sql/hazdb/README.md` and `sql/hazdb/pom.xml`
- Jet sample catalog: `jet/README.md`

