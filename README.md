# pipewatch

> A lightweight CLI monitor for tracking ETL pipeline health and alerting on stale or failed runs.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Register a pipeline and start watching:

```bash
# Register a pipeline with a max allowed staleness of 2 hours
pipewatch register --name "daily_sales_etl" --interval 2h

# Mark a pipeline run as successful
pipewatch checkin --name "daily_sales_etl"

# Check the status of all monitored pipelines
pipewatch status
```

Example output:

```
PIPELINE            LAST RUN         STATUS
daily_sales_etl     12 minutes ago   ✓ healthy
user_sync           3 hours ago      ✗ stale  [ALERT SENT]
orders_rollup       never            ✗ missing
```

Configure alerting via a simple config file:

```yaml
# pipewatch.yml
alerts:
  email: ops-team@example.com
  slack_webhook: https://hooks.slack.com/your/webhook
```

Then run the daemon:

```bash
pipewatch watch --config pipewatch.yml
```

---

## License

MIT © 2024 Your Name