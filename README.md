# Clearspend Database

## Abstract

This project establishes a modern, end-to-end data pipeline for ClearSpend, a high-growth fintech company processing millions of US-based credit and debit transactions. To resolve existing issues with fragmented infrastructure, inconsistent reporting, and unreliable raw production exports, this solution transforms "messy" operational data into a centralized, analytics-ready warehouse.

The architecture implements a multi-layer design—encompassing ingestion, transformation, and dimensional modeling—to provide high-fidelity data. By transitioning from manual Excel-based logic to a scalable star schema, the platform delivers automated, reproducible insights for the Finance, Customer Analytics, and Merchant Partnerships teams. Key business outcomes include the precise tracking of monthly revenue, customer lifetime value, and merchant performance metrics.

## Naming Conventions and Schema Architecture

To maintain structural consistency and programmatic accessibility, this repository adheres to the snake_case naming convention for all directories, files, and database objects.

Dimension Tables: Prefixed or identified with dim_ (e.g., dim_customers).

Fact Tables: Prefixed or identified with fact_ (e.g., fact_transactions).

