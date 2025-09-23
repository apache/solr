# Gradle Help Documentation

This directory contains text files that provide help documentation for various Gradle tasks and project workflows in Solr. 

## Purpose

These text files serve as the content for Gradle's help tasks. When users run commands like `./gradlew helpWorkflow` or `./gradlew helpTests`, Gradle displays the content of the corresponding text file in this directory.

## Available Help Topics

The following help topics are available:

* `ant.txt` - Information about Ant-Gradle migration
* `dependencies.txt` - Guide to declaring, inspecting and excluding dependencies
* `docker.txt` - Instructions for building Solr Docker images
* `forbiddenApis.txt` - How to add/apply rules for forbidden APIs
* `formatting.txt` - Code formatting conventions
* `git.txt` - Git assistance and guides
* `localSettings.txt` - Local settings, overrides and build performance tweaks
* `publishing.txt` - Release publishing, signing, etc.
* `tests.txt` - Information about tests, filtering, beasting, etc.
* `validateLogCalls.txt` - How to use logging calls efficiently
* `workflow.txt` - Typical workflow commands

## Usage

To view any of these help topics, run:

```bash
./gradlew help<Topic>
```

For example:

```bash
./gradlew helpWorkflow
./gradlew helpTests
./gradlew helpDeps
```

For a list of all available help commands, run:

```bash
./gradlew help
```

## Migration Note

These files were previously located in the `/help` directory at the project root. They were moved to `/dev-docs/gradle-help` to consolidate all documentation in the `dev-docs` directory while preserving Gradle's help functionality.