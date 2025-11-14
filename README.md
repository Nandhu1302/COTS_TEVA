# Class of Trade (CoT) Automation Processor

This project is a Python-based Flask API designed to automate the Class of Trade (CoT) process for Teva. It provides an asynchronous endpoint that accepts a file (CSV/Excel) of identifiers, processes them concurrently, and logs the job status in a PostgreSQL database.

## Features

* **Asynchronous API:** The main `/start-job` endpoint is fully asynchronous (`async`/`await`) to handle large volumes of data without blocking the server.
* **Concurrent Processing:** Uses `asyncio.gather` to process all identifiers in parallel, significantly reducing total processing time.
* **Synchronous Task Handling:** Safely runs blocking, synchronous database calls (using `psycopg2`) in separate threads via `asyncio.to_thread` to prevent freezing the server.
* **Database Tracking:** Logs all jobs and their individual identifier statuses in a PostgreSQL database.
* **Modular:** Includes a `dummy_async_function` as a template for adding future asynchronous modules.

## Tech Stack

* **Backend:** Flask (with `async` support)
* **Asynchronous Library:** `asyncio`
* **Database:** PostgreSQL
* **DB Driver:** `psycopg2`
* **File Parsing:** `pandas`
