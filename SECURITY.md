# Security Policy

## Supported Scope

Dropbox Cleaner is intended for local execution by the account owner on personal Dropbox accounts.

## Reporting A Security Issue

Please do not open public GitHub issues for security-sensitive findings.

When reporting a security issue, include:

- affected version or commit
- impact summary
- reproduction steps
- any suggested mitigation

## Sensitive Data

Please do not include:

- Dropbox access tokens
- refresh tokens
- personal file paths that should remain private
- logs containing sensitive Dropbox account data

## Project Security Principles

- no Dropbox password entry
- no silent overwrite of archive files
- no delete or move of originals in the staged archive workflow
- no token logging
- safe resume after interruption
