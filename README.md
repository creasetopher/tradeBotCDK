# TradeBot AWS Infrastructure

This repository contains the AWS cloud infrastructure that supports my tradebot. It uses the AWS Cloud Development Kit (CDK) with Python to define and manage the resources the bot runs on.

It includes infrastructure and supporting services for:

- Finding stocks to watch and collecting live market data.
- Storing market history, bot state, and order records.
- Processing trade orders and connecting to a broker.
- Scheduling background jobs, passing messages between services, and managing runtime controls.

The main infrastructure definition lives in `src/trade_bot_cdk/`, with supporting service code in `src/` and tests in `tests/`.
