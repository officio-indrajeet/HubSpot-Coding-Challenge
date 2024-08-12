import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * A program to calculate the maximum number of concurrent calls for each customer on each day,
 * based on call data retrieved from an HTTP GET endpoint. The results are sent to an HTTP POST endpoint.
 */
public class HubSpotCodingChallenge {

    /**
     * Represents a phone call record.
     */
    static class Call {
        int customerId;
        String callId;
        long startTimestamp;
        long endTimestamp;

        /**
         * Constructs a Call object.
         *
         * @param customerId     Unique identifier for the customer.
         * @param callId         Unique identifier for the call.
         * @param startTimestamp UNIX timestamp (in milliseconds) when the call started.
         * @param endTimestamp   UNIX timestamp (in milliseconds) when the call ended.
         */
        public Call(int customerId, String callId, long startTimestamp, long endTimestamp) {
            this.customerId = customerId;
            this.callId = callId;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
        }
    }

    /**
     * Represents the result of maximum concurrent calls for a customer on a specific date.
     */
    static class Result {
        int customerId;
        String date;
        int maxConcurrentCalls;
        long timestamp;
        List<String> callIds;

        /**
         * Constructs a Result object.
         *
         * @param customerId         Unique identifier for the customer.
         * @param date               Date for which the result is calculated, in "YYYY-MM-DD" format.
         * @param maxConcurrentCalls Maximum number of concurrent calls on the given date.
         * @param timestamp          UNIX timestamp (in milliseconds) during the peak concurrency period.
         * @param callIds            List of call IDs active during the peak concurrency.
         */
        public Result(int customerId, String date, int maxConcurrentCalls, long timestamp, List<String> callIds) {
            this.customerId = customerId;
            this.date = date;
            this.maxConcurrentCalls = maxConcurrentCalls;
            this.timestamp = timestamp;
            this.callIds = callIds;
        }

        /**
         * Converts the result to a JSON object.
         *
         * @return JSONObject representing the result.
         */
        public JSONObject toJSON() {
            JSONObject json = new JSONObject();
            json.put("customerId", customerId);
            json.put("date", date);
            json.put("maxConcurrentCalls", maxConcurrentCalls);
            json.put("timestamp", timestamp);
            json.put("callIds", new JSONArray(callIds));
            return json;
        }
    }

    public static void main(String[] args) {
        String getEndpoint = "https://candidate.hubteam.com/candidateTest/v3/problem/dataset?userKey=59d6451f8b4135c2f704e5163362";
        String postEndpoint = "https://candidate.hubteam.com/candidateTest/v3/problem/result?userKey=59d6451f8b4135c2f704e5163362";

        try {
            // Fetch call data
            List<Call> calls = fetchCallData(getEndpoint);

            // Process data
            Map<Integer, Map<String, List<Call>>> groupedCalls = groupCallsByCustomerAndDate(calls);
            List<Result> results = calculateMaxConcurrentCalls(groupedCalls);

            // Post results
            postResults(postEndpoint, results);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Fetches call data from the specified GET endpoint.
     *
     * @param endpoint URL of the GET endpoint.
     * @return List of Call objects retrieved from the endpoint.
     * @throws Exception if there is an error during the HTTP request or response parsing.
     */
    private static List<Call> fetchCallData(String endpoint) throws Exception {
        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");

        if (conn.getResponseCode() != 200) {
            throw new RuntimeException("Failed: HTTP error code : " + conn.getResponseCode());
        }

        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
        StringBuilder sb = new StringBuilder();
        String output;
        while ((output = br.readLine()) != null) {
            sb.append(output);
        }

        conn.disconnect();

        // Parse the response as a JSON object
        JSONObject jsonObject = new JSONObject(sb.toString());
        JSONArray jsonArray = jsonObject.getJSONArray("callRecords"); // Adjusted key based on response structure

        List<Call> calls = new ArrayList<>();

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject callObject = jsonArray.getJSONObject(i);
            int customerId = callObject.getInt("customerId");
            String callId = callObject.getString("callId");
            long startTimestamp = callObject.getLong("startTimestamp");
            long endTimestamp = callObject.getLong("endTimestamp");
            calls.add(new Call(customerId, callId, startTimestamp, endTimestamp));
        }

        return calls;
    }

    /**
     * Groups calls by customer ID and date.
     *
     * @param calls List of Call objects.
     * @return A map with customer ID as the key, and a map of date to list of calls as the value.
     */
    private static Map<Integer, Map<String, List<Call>>> groupCallsByCustomerAndDate(List<Call> calls) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Map<Integer, Map<String, List<Call>>> groupedCalls = new HashMap<>();

        for (Call call : calls) {
            // Determine the date range for this call
            String startDate = dateFormat.format(new Date(call.startTimestamp));
            String endDate = dateFormat.format(new Date(call.endTimestamp));

            // Add call for each day it spans
            for (String date = startDate; !date.equals(endDate); date = incrementDate(date)) {
                groupedCalls.computeIfAbsent(call.customerId, k -> new HashMap<>())
                        .computeIfAbsent(date, k -> new ArrayList<>())
                        .add(call);
            }
            // Ensure the call is added for the end date as well
            groupedCalls.computeIfAbsent(call.customerId, k -> new HashMap<>())
                    .computeIfAbsent(endDate, k -> new ArrayList<>())
                    .add(call);
        }

        return groupedCalls;
    }

    /**
     * Increments the given date by one day.
     *
     * @param date Date in "yyyy-MM-dd" format.
     * @return The incremented date in "yyyy-MM-dd" format.
     */
    private static String incrementDate(String date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date currentDate = dateFormat.parse(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(currentDate);
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            return dateFormat.format(calendar.getTime());
        } catch (Exception e) {
            throw new RuntimeException("Failed to increment date: " + date, e);
        }
    }

    /**
     * Calculates the maximum number of concurrent calls for each customer per date.
     *
     * @param groupedCalls A map with customer ID as the key, and a map of date to list of calls as the value.
     * @return List of Result objects representing maximum concurrent calls per customer per date.
     */
    private static List<Result> calculateMaxConcurrentCalls(Map<Integer, Map<String, List<Call>>> groupedCalls) {
        List<Result> results = new ArrayList<>();

        for (int customerId : groupedCalls.keySet()) {
            for (String date : groupedCalls.get(customerId).keySet()) {
                List<Call> calls = groupedCalls.get(customerId).get(date);

                // Find maximum concurrent calls
                int maxConcurrent = 0;
                long maxTimestamp = 0;
                List<String> maxConcurrentCallIds = new ArrayList<>();

                // Create events for each start and end of a call
                List<long[]> events = new ArrayList<>();
                for (Call call : calls) {
                    // Add event for start
                    long callStart = call.startTimestamp;
                    long callEnd = call.endTimestamp;
                    // Clamp events to the current date
                    long startOfDay = getStartOfDayInMillis(date);
                    long endOfDay = getEndOfDayInMillis(date);
                    if (callStart < startOfDay) callStart = startOfDay;
                    if (callEnd > endOfDay) callEnd = endOfDay;

                    events.add(new long[]{callStart, 1, calls.indexOf(call)});
                    events.add(new long[]{callEnd, -1, calls.indexOf(call)});
                }

                // Sort events by time; if times are equal, end event (-1) comes before start event (1)
                events.sort((e1, e2) -> {
                    if (e1[0] != e2[0]) return Long.compare(e1[0], e2[0]);
                    return Long.compare(e1[1], e2[1]);
                });

                // Track concurrent calls
                int currentConcurrent = 0;
                Map<String, Long> callIdToStartTime = new HashMap<>();
                Set<String> currentCallIds = new HashSet<>();

                for (long[] event : events) {
                    long timestamp = event[0];
                    int type = (int) event[1];
                    String callId = calls.get((int) event[2]).callId;
                    long callStart = calls.get((int) event[2]).startTimestamp;

                    // Update concurrent calls
                    if (type == 1) {
                        currentConcurrent++;
                        currentCallIds.add(callId);
                        callIdToStartTime.put(callId, callStart);
                    } else {
                        currentConcurrent--;
                        currentCallIds.remove(callId);
                    }

                    // Check if the current concurrency is the maximum
                    if (currentConcurrent > maxConcurrent) {
                        maxConcurrent = currentConcurrent;
                        maxTimestamp = timestamp;
                        maxConcurrentCallIds = new ArrayList<>(currentCallIds);
                    }
                }

                if (maxConcurrent > 0) { // Only add results if there were calls on that day
                    // Sort the call IDs by their original start timestamp to ensure consistent ordering
                    maxConcurrentCallIds.sort(Comparator.comparing(callIdToStartTime::get));
                    results.add(new Result(customerId, date, maxConcurrent, maxTimestamp, maxConcurrentCallIds));
                }
            }
        }

        return results;
    }

    /**
     * Calculates the start of the day in milliseconds for a given date.
     *
     * @param date Date in "yyyy-MM-dd" format.
     * @return UNIX timestamp in milliseconds representing the start of the day.
     */
    private static long getStartOfDayInMillis(String date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date dayStart = dateFormat.parse(date);
            return dayStart.getTime();
        } catch (Exception e) {
            throw new RuntimeException("Error calculating start of day: " + date, e);
        }
    }

    /**
     * Calculates the end of the day in milliseconds for a given date.
     *
     * @param date Date in "yyyy-MM-dd" format.
     * @return UNIX timestamp in milliseconds representing the end of the day.
     */
    private static long getEndOfDayInMillis(String date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date dayStart = dateFormat.parse(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(dayStart);
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            calendar.add(Calendar.MILLISECOND, -1);
            return calendar.getTimeInMillis();
        } catch (Exception e) {
            throw new RuntimeException("Error calculating end of day: " + date, e);
        }
    }

    /**
     * Sends the calculated results to the specified POST endpoint and logs the response.
     *
     * @param endpoint URL of the POST endpoint.
     * @param results  List of Result objects to be sent.
     * @throws Exception if there is an error during the HTTP request.
     */
    private static void postResults(String endpoint, List<Result> results) throws Exception {
        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");

        JSONArray jsonResults = new JSONArray();
        for (Result result : results) {
            jsonResults.put(result.toJSON());
        }

        JSONObject payload = new JSONObject();
        payload.put("results", jsonResults);

        // Log the payload before sending
        System.out.println("Payload: " + payload.toString(2));

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = payload.toString().getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        // Log the response from the server
        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
            System.out.println("POST request successful.");
            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            System.out.println("Response: " + response.toString()); // Log the success response
        } else {
            System.out.println("POST request failed. Response code: " + responseCode);
            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            System.out.println("Error response: " + response.toString()); // Log the error response
        }

        conn.disconnect();
    }

}
