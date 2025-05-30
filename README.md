# Shardy

## Motivation

Inspired with tantivy, I wanted to try something similar but a lot simpler - with sqlite as a backend.

## Overview

Shardy is a log analytics software where compute is decoupled from storage.

It uses distributed sqlite shards stored on s3 for log analytics and querying.

## Architecture

Single master mode accepts logs and queries, and then distributes them to n workers.

The workers are capable of indexing and querying logs stored on s3 as smaller (hundreds of megabytes) shards.
