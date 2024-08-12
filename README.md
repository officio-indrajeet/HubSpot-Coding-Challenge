# HubSpot Concurrent Calls Billing Challenge

## Overview

This project calculates the peak number of concurrent calls made by customers using HubSpot's calling capabilities. The objective is to bill customers based on their peak calling load by determining the maximum number of concurrent calls each customer makes on a given day.

## Problem Statement

Sales representatives use HubSpot to make calls to prospects throughout the day. Some customers have many sales reps making calls concurrently, which can strain the system. The goal is to calculate the maximum concurrent calls for each customer per day and submit this data to a specified endpoint.

## Implementation

The implementation involves two main steps:

1. **Data Retrieval:** Fetch call records from a GET endpoint provided by HubSpot.
2. **Data Processing:** Calculate the maximum concurrent calls for each customer for each day.
3. **Data Submission:** Send the calculated results to a POST endpoint.

## API Endpoints

- **GET Endpoint:**
  - URL: `https://candidate.hubteam.com/candidateTest/v3/problem/dataset?userKey=59d6451f8b4135c2f704e5163362`
  - Retrieves call records as JSON objects.

- **POST Endpoint:**
  - URL: `https://candidate.hubteam.com/candidateTest/v3/problem/result?userKey=59d6451f8b4135c2f704e5163362`
  - Accepts JSON data containing the maximum concurrent calls per customer per day.

## JSON Structure

### Input

Each call record has the following fields:

- `customerId`: A unique identifier for the customer.
- `callId`: A unique identifier for the call.
- `startTimestamp`: UNIX timestamp in milliseconds when the call started.
- `endTimestamp`: UNIX timestamp in milliseconds when the call ended.

### Output

The result JSON structure is as follows:

```json
{
  "results": [
    {
      "customerId": 123,
      "date": "2024-02-07",
      "maxConcurrentCalls": 3,
      "timestamp": 1707314726000,
      "callIds": [
        "2c269d25-deb9-42cf-927c-543112f7a76b",
        "3c569d35-deb9-42cf-927c-543112f7a75c",
        "4f679d45-deb9-42cf-927c-543112f7a77d"
      ]
    }
  ]
}
