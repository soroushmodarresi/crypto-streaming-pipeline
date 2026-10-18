\# Real-Time Cryptocurrency Streaming Pipeline



A real-time data engineering project that collects cryptocurrency prices, streams events through Redpanda, and stores them in Microsoft SQL Server for analysis.



\## Architecture



CoinGecko API → Python Producer → Redpanda (Kafka-compatible) → Python Consumer → SQL Server



\### Data flow



1\. \*\*Ingestion:\*\* The producer fetches USD prices for Bitcoin (BTC), Ethereum (ETH), and Solana (SOL).

2\. \*\*Streaming:\*\* Price events are published to the `crypto-prices` topic in Redpanda.

3\. \*\*Validation:\*\* Events are checked for required fields, supported symbols, and valid price ranges.

4\. \*\*Storage:\*\* The consumer inserts validated events into SQL Server.

5\. \*\*Testing:\*\* Automated unit tests verify the event-validation rules.



\## Technology Stack



\- Python 3.10

\- Redpanda / Kafka-compatible event streaming

\- Microsoft SQL Server 2022

\- SQL Server Management Studio (SSMS)

\- `confluent-kafka`

\- `requests`

\- `pyodbc`

\- `python-dotenv`

\- `unittest`



\## Database Schema



Database: `CryptoDB`



Table: `dbo.CryptoPrices`



| Column | Description |

|---|---|

| `Id` | Auto-incrementing row ID |

| `Symbol` | Cryptocurrency symbol |

| `PriceUsd` | Price in USD |

| `EventTimestamp` | Timestamp from the event |

| `Source` | Price data source |

| `IngestedAt` | Database ingestion timestamp |



\## Prerequisites



\- Windows

\- Python 3.10 or compatible version

\- Docker Desktop

\- SQL Server ODBC Driver 18

\- SQL Server Management Studio (optional, for querying data)



\## Setup



1\. Clone this repository.

2\. Create and activate a virtual environment.

3\. Install dependencies.

4\. Configure your environment variables in a local `.env` file.

5\. Start the Docker services.

6\. Create the `CryptoDB` database and `dbo.CryptoPrices` table.

7\. Start the consumer and producer in separate terminals.



\### Install dependencies



```powershell

python -m venv .venv

.\\.venv\\Scripts\\Activate.ps1

pip install confluent-kafka requests pyodbc python-dotenv

```



\### Environment configuration



Create a local `.env` file:



```text

MSSQL\_SA\_PASSWORD=your\_local\_strong\_password

MSSQL\_HOST=localhost

MSSQL\_PORT=1433

MSSQL\_DATABASE=CryptoDB

MSSQL\_USER=sa

```



Never commit `.env` or real credentials to version control.



\### Start the services



```powershell

docker compose up -d

```



\### Run the pipeline



Open two PowerShell terminals from the project root.



Terminal 1 — consumer:



```powershell

.\\.venv\\Scripts\\Activate.ps1

python src\\consumer.py

```



Terminal 2 — producer:



```powershell

.\\.venv\\Scripts\\Activate.ps1

python src\\producer.py

```



The producer fetches prices approximately every 10 seconds.



\## Run Tests



```powershell

python -m unittest discover -s tests -v

```



The validation tests cover valid events, unsupported symbols, invalid prices, missing fields, and malformed input.



\## Verify the Data



Run the following query in SQL Server Management Studio:



```sql

USE CryptoDB;



SELECT TOP 20

&#x20;   Id,

&#x20;   Symbol,

&#x20;   PriceUsd,

&#x20;   EventTimestamp,

&#x20;   Source,

&#x20;   IngestedAt

FROM dbo.CryptoPrices

ORDER BY Id DESC;

```



\## Security Notes



\- Credentials belong in `.env`, not source code.

\- `.env` and `.venv/` should be excluded through `.gitignore`.

\- The SQL Server configuration is intended for local development. Review authentication, network exposure, and encryption before deploying elsewhere.



\## Future Improvements



\- Add structured logging and retry/backoff policies.

\- Add automated integration tests.

\- Add duplicate-event handling and idempotent writes.

\- Build a dashboard for cryptocurrency price trends.

\- Add CI with GitHub Actions.

\- Containerize the Python producer and consumer.



