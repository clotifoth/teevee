## Problem

For this problem, you will develop a few HTTP APIs to perform some simple functions on daily stock prices. For the source of prices, please use the AlphaVantage
[TIME_SERIES_DAILY_ADJUSTED](https://www.alphavantage.co/documentation/#dailyadj) API endpoint. You can get a free API key for AlphaVantage from [here](https://www.alphavantage.co/support/#api-key). Note that this key will be limited to 500 calls per day.

We estimate that this should take no more than a couple of hours to complete (perhaps slightly longer for anyone using Python for the first time).  Feel free to make compromises based on time-boxing this to a couple of hours. This exercise is meant to get a general sense of how you write software and how you design APIs; it's not meant to be a bulletproof production solution.

## Requirements

Create the following 3 endpoints:

1. `lookup`: given a symbol and a date, return the open, high, low, close, and volume for that symbol on that date. Response should be in this format:
```json
{ "open":   127.1000, 
  "high":   128.2900,
  "low":    126.5300,
  "close":  127.9600,
  "volume": 3671903 }
```
2. `min`: given a symbol and a range 'n', return the lowest price that symbol traded at over the last 'n' data points (lowest low). Response should be in this format:
```json
{"min": 122.685}
```
3. `max`: given a symbol and a range 'n', return the highest price that symbol traded at over the last 'n' data points (highest high). Response should be in this format:
```json
{"max": 128.93}
```

A lazy-loaded cache should be implemented. The first time a given symbol is requested, hit the AlphaVantage API and cache the results. Subsequent requests for the same symbol should be served out of the cache.

The program should embed a web server and accept HTTP requests. For example, you can use Flask.

## Deliverables

### Software
A solution implemented in Python with instructions on how to run locally and how to call the endpoints via `curl`.

### Discussion
A short write-up where you discuss the following:

- What compromises did you make due to time constraints?
- What would you do differently if this software was meant for production use?
- Propose how you might implement authentication, such that only authorized users may hit these endpoints.
- How much time did you spend on this exercise?
- Please include any other comments about your implementation.
- Please include any general feedback you have about the exercise.
